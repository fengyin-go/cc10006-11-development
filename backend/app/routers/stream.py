"""流式处理接口：维护流处理作业，并暴露检查点恢复与恢复进度查询。

动作接口维持 ``POST /api/stream/{id}/actions`` 的统一约定，同时把检查点相关能力
单独开放成可查询的只读接口，便于恢复面板、回填台账和值班排查共用同一份事实。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.schemas import ActionResult, EntryPayload, PageResult
from app.services.checkpoint import checkpoint_service
from app.services.stream import StreamService

router = APIRouter(prefix="/api/stream", tags=["流式处理"])

service = StreamService()

LIST_FIELDS = ["作业编号", "输入Topic", "处理逻辑", "窗口类型", "窗口时长秒", "水位延迟", "检查点间隔", "输出Topic"]
STATUSES = ["草稿", "待校验", "运行中", "已停止"]
CHECKPOINT_ACTIONS = {"提交检查点", "恢复作业"}


@router.get("/recovery", response_model=dict)
def list_recovery_records(job_no: str | None = Query(default=None, description="按作业编号定位恢复记录")) -> dict:
    """按作业编号查询恢复记录；不传编号时汇总所有作业的恢复面板数据。"""
    records = checkpoint_service.recovery_records(job_no)
    if not records.get("ok"):
        raise HTTPException(status_code=404, detail=str(records.get("message")))
    return records


@router.get("/recovery/{job_id}", response_model=dict)
def get_recovery_progress(job_id: int) -> dict:
    """恢复面板：最后成功水位、检查点间隔、剩余待处理事件、本次/上一次检查点与输出回执。"""
    progress = checkpoint_service.progress_by_job_id(job_id)
    if progress is None:
        raise HTTPException(status_code=404, detail=f"流处理作业 {job_id} 不存在或没有检查点")
    return progress


@router.get("/export")
def export_entries() -> dict[str, Any]:
    """导出流式处理清单：返回当前过滤条件下的全量数据。"""
    items, total = service.list_entries(page=1, size=10000)
    return {"module": "stream", "total": total, "items": items}


@router.get("", response_model=PageResult[dict])
def list_entries(
    keyword: str | None = Query(default=None, description="按作业编号检索"),
    status: str | None = Query(default=None, description="草稿、待校验、运行中、已停止"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """按作业编号与状态过滤流式处理列表；没有数据时返回空页，不报错。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = service.list_entries(keyword=keyword, status=status, page=page, size=size)
    # 列表字段与检查点事实对齐：已确认水位、间隔、恢复状态、剩余事件都来自同一份持久化状态
    for row in items:
        progress = checkpoint_service.progress_by_job_id(int(row.get("id", 0)))
        if progress:
            row["已确认水位"] = progress["最后成功水位"]
            row["检查点间隔"] = progress["检查点间隔"]
            row["恢复状态"] = progress["恢复状态"]
            row["最后成功检查点"] = progress["最后成功检查点"]["检查点编号"] if progress["最后成功检查点"] else "—"
            row["剩余待处理事件"] = progress["剩余待处理事件"]
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条流处理作业明细；不存在时给出可读的错误说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"流处理作业 {entry_id} 不存在或已归档")
    progress = checkpoint_service.progress_by_job_id(entry_id)
    if progress:
        entry["检查点"] = progress
        entry["已确认水位"] = progress["最后成功水位"]
        entry["检查点间隔"] = progress["检查点间隔"]
        entry["恢复状态"] = progress["恢复状态"]
        entry["剩余待处理事件"] = progress["剩余待处理事件"]
    return entry


@router.post("", response_model=ActionResult)
def create_entry(payload: EntryPayload) -> ActionResult:
    """登记一条流处理作业，缺字段时说明原因而不是静默丢弃。"""
    entry, missing = service.create_entry(payload.values)
    if missing:
        if len(missing) == 1 and missing[0].startswith("作业编号已被占用"):
            return ActionResult(ok=False, message=missing[0])
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    progress = checkpoint_service.progress_by_job_id(int(entry.get("id", 0)))
    if progress:
        entry["检查点"] = progress
    return ActionResult(ok=True, message="流处理作业已登记", entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: EntryPayload) -> ActionResult:
    """执行提交校验、启动作业、停止作业，以及检查点相关的提交检查点/恢复作业。

    检查点写入与输出回执、作业状态、下游回填台账在同一事务内提交；失败时整体回滚到
    可恢复状态，并保留最后成功检查点。
    """
    values = dict(payload.values)
    action = str(values.get("action") or "").strip()
    if action == "提交检查点":
        ok, message, progress = checkpoint_service.commit_checkpoint(entry_id, values)
        entry = service.get_entry(entry_id)
        if entry is None:
            return ActionResult(ok=False, message=message)
        return ActionResult(ok=ok, message=message, entry={"作业": entry, "恢复进度": progress})
    if action in CHECKPOINT_ACTIONS:
        ok, message, progress = checkpoint_service.request_recovery(entry_id, values)
        entry = service.get_entry(entry_id)
        if entry is None:
            return ActionResult(ok=False, message=message)
        return ActionResult(ok=ok, message=message, entry={"作业": entry, "恢复进度": progress})
    entry, message = service.run_action(entry_id, action)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)
