"""流处理检查点与恢复：按输入 Topic 持久化水位、检查点间隔与输出回执。

这个模块是流作业恢复的唯一事实来源：
- 检查点状态、输出回执落盘（默认 backend/var/checkpoints.json），作业进程重启后仍可恢复；
- 写入检查点、作业状态、下游回填台账与审计记录在同一把锁内一起提交，任何一步失败都回到
  上一次成功检查点；
- 恢复只从最后成功检查点继续，重复/并发的恢复请求只允许一个实例生效；
- 非法水位（非整数、低于最后成功检查点、超出输入 Topic 范围）一律拒绝。
"""
from __future__ import annotations

import copy
import json
import os
import tempfile
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.config import settings
from app.store import store

STREAM_MODULE = "stream"
BACKFILL_MODULE = "backfill"
AUDIT_MODULE = "audit"

DEFAULT_CHECKPOINT_INTERVAL = 30
DEFAULT_INPUT_RATE = 2000
MAX_CHECKPOINT_INTERVAL = 86400
# 并发/重复恢复请求的合并窗口：同一目标检查点的重复请求只保留一个实例
DEDUP_WINDOW_SECONDS = 30


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _parse_int(value: Any) -> int | None:
    """只接受真正的整数（含纯数字字符串），小数、布尔与其它文本都视为非法。"""
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value) if value.is_integer() else None
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(text, 10)
    except ValueError:
        return None


def _parse_interval(value: Any, fallback: int = DEFAULT_CHECKPOINT_INTERVAL) -> int | None:
    """检查点间隔必须是正整数；无法解析时返回 fallback，fallback 为负表示判为非法。"""
    parsed = _parse_int(value)
    if parsed is None:
        return None if fallback < 0 else fallback
    if parsed <= 0 or parsed > MAX_CHECKPOINT_INTERVAL:
        return None
    return parsed


def _positive(value: Any, fallback: int) -> int:
    parsed = _parse_int(value)
    return parsed if parsed is not None and parsed >= 0 else fallback


def _checkpoint_summary(checkpoint: dict[str, Any] | None) -> dict[str, Any] | None:
    if not checkpoint:
        return None
    return {
        "检查点编号": checkpoint.get("checkpoint_id"),
        "水位": checkpoint.get("watermark"),
        "起始水位": checkpoint.get("from_watermark"),
        "事件数": checkpoint.get("event_count"),
        "输出编号区间": checkpoint.get("output_range"),
        "状态": checkpoint.get("status"),
        "已完成": bool(checkpoint.get("completed")),
        "创建时间": checkpoint.get("created_at"),
        "完成时间": checkpoint.get("completed_at"),
    }


class CheckpointService:
    """检查点仓库：内存状态 + 原子落盘 + 线程锁保证并发请求串行提交。"""

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)
        self._lock = threading.RLock()
        self._data: dict[str, Any] | None = None

    # ------------------------------------------------------------------ 持久化
    def _load(self) -> dict[str, Any]:
        if self._data is not None:
            return self._data
        if self._path.exists():
            try:
                loaded = json.loads(self._path.read_text(encoding="utf-8"))
                if isinstance(loaded, dict) and "topics" in loaded and "jobs" in loaded:
                    self._data = loaded
                    self._data.setdefault("receipts", {})
                    self._repair(self._data)
                    return self._data
            except (OSError, ValueError):
                # 文件损坏时退回示例数据，避免整个服务不可用
                pass
        self._data = self._bootstrap()
        self._repair(self._data)
        self._persist(self._data)
        return self._data

    def _persist(self, data: dict[str, Any]) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True)
        fd, tmp_path = tempfile.mkstemp(prefix=".checkpoints-", suffix=".json", dir=str(self._path.parent))
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp_path, self._path)
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

    def _bootstrap(self) -> dict[str, Any]:
        """从流作业示例数据生成初始状态：已经存在的检查点原样沿用到面板。"""
        data: dict[str, Any] = {
            "version": 1,
            "topics": {},
            "jobs": {},
            "receipts": {},
        }
        for row in store.rows(STREAM_MODULE):
            job_no = str(row.get("作业编号") or "").strip()
            input_topic = str(row.get("输入Topic") or "").strip()
            if not job_no or not input_topic:
                continue
            topic = self._ensure_topic(data, input_topic, row, seed=True)
            self._ensure_job(data, row, topic)
        return data

    def _ensure_topic(
        self,
        data: dict[str, Any],
        input_topic: str,
        job_row: dict[str, Any],
        *,
        seed: bool = False,
    ) -> dict[str, Any]:
        topic = data["topics"].get(input_topic)
        if topic is not None:
            return topic
        interval = _parse_interval(job_row.get("检查点间隔"))
        if interval is None:
            interval = DEFAULT_CHECKPOINT_INTERVAL
        confirmed = _positive(job_row.get("已确认水位"), 0)
        total = _positive(job_row.get("输入总量"), 0)
        rate = _positive(job_row.get("输入速率"), DEFAULT_INPUT_RATE)
        checkpoints: list[dict[str, Any]] = []
        last_success: dict[str, Any] | None = None
        seed_last_id = str(job_row.get("最后成功检查点") or "").strip()
        if seed and confirmed > 0 and seed_last_id:
            previous_id = str(job_row.get("上一次成功检查点") or "").strip()
            previous_watermark = _positive(job_row.get("上一次成功水位"), max(confirmed - interval * rate, 0))
            if previous_id and previous_watermark > 0:
                checkpoints.append(
                    {
                        "checkpoint_id": previous_id,
                        "watermark": previous_watermark,
                        "from_watermark": 0,
                        "event_count": previous_watermark,
                        "output_range": [1, previous_watermark],
                        "status": "已完成",
                        "completed": True,
                        "created_at": "2026-11-13T08:00:00+00:00",
                        "completed_at": "2026-11-13T08:00:00+00:00",
                        "job_no": str(job_row.get("作业编号") or ""),
                    }
                )
            last_success = {
                "checkpoint_id": seed_last_id,
                "watermark": confirmed,
                "from_watermark": previous_watermark,
                "event_count": confirmed - previous_watermark,
                "output_range": [previous_watermark + 1, confirmed],
                "status": "已完成",
                "completed": True,
                "created_at": "2026-11-13T08:30:00+00:00",
                "completed_at": "2026-11-13T08:30:00+00:00",
                "job_no": str(job_row.get("作业编号") or ""),
            }
            checkpoints.append(last_success)
            if job_row.get("输出Topic"):
                self._seed_receipt(
                    data,
                    str(job_row.get("输出Topic")),
                    str(job_row.get("作业编号") or ""),
                    input_topic,
                    checkpoints,
                )
        topic = {
            "input_topic": input_topic,
            "confirmed_watermark": confirmed,
            "checkpoint_interval": interval,
            "input_total_events": total,
            "input_rate": rate,
            # 输入 Topic 的消费租约：同一输入 Topic 只允许一个作业实例持有水位与输出回执
            "active_job_no": str(job_row.get("作业编号") or "") if checkpoints else None,
            "last_success_checkpoint": last_success,
            "checkpoints": checkpoints,
            "next_sequence": len(checkpoints) + 1,
        }
        data["topics"][input_topic] = topic
        return topic

    def _seed_receipt(
        self,
        data: dict[str, Any],
        output_topic: str,
        job_no: str,
        input_topic: str,
        checkpoints: list[dict[str, Any]],
    ) -> None:
        receipt = data["receipts"].setdefault(
            output_topic,
            {"output_topic": output_topic, "written_ranges": [], "deduplicated_events": 0},
        )
        for checkpoint in checkpoints:
            output_range = checkpoint.get("output_range")
            if not checkpoint.get("completed") or not output_range:
                continue
            receipt["written_ranges"].append(
                {
                    "start": output_range[0],
                    "end": output_range[1],
                    "checkpoint_id": checkpoint.get("checkpoint_id"),
                    "job_no": job_no,
                    "input_topic": input_topic,
                    "recorded_at": checkpoint.get("completed_at"),
                }
            )

    def _ensure_job(self, data: dict[str, Any], job_row: dict[str, Any], topic: dict[str, Any]) -> dict[str, Any]:
        job_no = str(job_row.get("作业编号") or "").strip()
        job = data["jobs"].get(job_no)
        if job is not None:
            return job
        job = {
            "job_id": int(job_row.get("id", 0)),
            "job_no": job_no,
            "input_topic": topic["input_topic"],
            "output_topic": str(job_row.get("输出Topic") or "").strip(),
            "active_instance": None,
            "recovery": {
                "instance_id": None,
                "status": "未发起",
                "completed": False,
                "requested_at": None,
                "completed_at": None,
                "message": "尚未发起恢复",
            },
            "instances": [],
            "next_instance_sequence": 1,
        }
        # 示例数据里已经恢复过的作业，重启后恢复面板要显示上次的恢复结果
        if topic.get("last_success_checkpoint") and str(job_row.get("恢复状态") or "") == "已恢复":
            job["recovery"] = {
                "instance_id": None,
                "status": "已恢复",
                "completed": True,
                "requested_at": None,
                "completed_at": topic["last_success_checkpoint"].get("completed_at"),
                "message": "上次运行已从最后成功检查点恢复",
            }
        data["jobs"][job_no] = job
        return job

    def _repair(self, data: dict[str, Any]) -> None:
        """加载后对齐三方视图：最后成功检查点优先级最高，输出回执不得越过它。"""
        for topic in data.get("topics", {}).values():
            last = topic.get("last_success_checkpoint")
            checkpoints = topic.setdefault("checkpoints", [])
            if last and last.get("completed"):
                # 已确认水位只能等于最后成功检查点的水位
                topic["confirmed_watermark"] = last.get("watermark", topic.get("confirmed_watermark", 0))
            else:
                completed = [cp for cp in checkpoints if cp.get("completed")]
                topic["last_success_checkpoint"] = completed[-1] if completed else None
                if topic["last_success_checkpoint"]:
                    topic["confirmed_watermark"] = topic["last_success_checkpoint"].get("watermark", 0)
            topic.setdefault("checkpoint_interval", DEFAULT_CHECKPOINT_INTERVAL)
            topic.setdefault("input_total_events", 0)
            topic.setdefault("input_rate", DEFAULT_INPUT_RATE)
            topic.setdefault("next_sequence", len(checkpoints) + 1)
            topic.setdefault("active_job_no", None)
            if topic["active_job_no"] is None and last:
                topic["active_job_no"] = last.get("job_no")
        for job in data.get("jobs", {}).values():
            job.setdefault("instances", [])
            job.setdefault("recovery", {"instance_id": None, "status": "未发起", "completed": False})
            job.setdefault("active_instance", None)
        # 输出回执必须与最后成功检查点对齐：超出最后成功水位的写入区间一律裁掉，
        # 冲突时以最后成功检查点为准，保证恢复后不会把未检查点的编号算作已确认。
        for topic in data.get("topics", {}).values():
            last = topic.get("last_success_checkpoint")
            if not last or not last.get("completed"):
                continue
            watermark = int(last.get("watermark", 0) or 0)
            topic_name = topic.get("input_topic")
            for receipt in data.get("receipts", {}).values():
                kept: list[dict[str, Any]] = []
                for item in receipt.get("written_ranges", []):
                    if str(item.get("input_topic") or "") != str(topic_name):
                        kept.append(item)
                        continue
                    start, end = int(item.get("start", 0)), int(item.get("end", 0))
                    if start > watermark:
                        receipt["truncated_events"] = int(receipt.get("truncated_events", 0) or 0) + (end - start + 1)
                        continue
                    if end > watermark:
                        receipt["truncated_events"] = int(receipt.get("truncated_events", 0) or 0) + (end - watermark)
                        item = {**item, "end": watermark}
                    kept.append(item)
                receipt["written_ranges"] = sorted(kept, key=lambda row: int(row.get("start", 0)))
        for output_topic, receipt in data.get("receipts", {}).items():
            valid = [item for item in receipt.get("written_ranges", []) if item.get("start") and item.get("end")]
            receipt["written_ranges"] = valid
            receipt.setdefault("output_topic", output_topic)
            receipt.setdefault("deduplicated_events", 0)

    # ------------------------------------------------------------------ 查询
    def _state(self) -> dict[str, Any]:
        with self._lock:
            return self._load()

    def _resolve(self, job_id: int | None = None, job_no: str | None = None) -> tuple[dict[str, Any] | None, dict[str, Any] | None, dict[str, Any]]:
        data = self._state()
        row: dict[str, Any] | None = None
        if job_id is not None:
            row = store.find(STREAM_MODULE, job_id)
        elif job_no:
            row = next((item for item in store.rows(STREAM_MODULE) if str(item.get("作业编号")) == job_no), None)
        if row is None:
            return None, None, data
        job_no_value = str(row.get("作业编号") or "").strip()
        input_topic = str(row.get("输入Topic") or "").strip()
        if not job_no_value or not input_topic:
            return None, None, data
        with self._lock:
            topic = self._ensure_topic(data, input_topic, row)
            job = self._ensure_job(data, row, topic)
            # 首次补齐的作业/输入 Topic 也要落盘，重启后状态不丢
            if not self._path.exists():
                self._persist(data)
        return row, job, data

    def _sync_rows(self, job: dict[str, Any], topic: dict[str, Any], data: dict[str, Any]) -> None:
        """把检查点事实回填到各业务面的列表字段，读到的三个面因此始终一致。"""
        holder = str(topic.get("active_job_no") or "") == str(job.get("job_no")) or topic.get("active_job_no") is None
        last = topic.get("last_success_checkpoint") if holder else None
        confirmed = int(topic.get("confirmed_watermark", 0) or 0) if holder else 0
        remaining = max(int(topic.get("input_total_events", 0) or 0) - confirmed, 0)
        # 只统计属于当前作业的检查点，避免同一输入 Topic 被复用后串到别的作业
        checkpoints = self._job_checkpoints(topic, job)
        latest = checkpoints[-1] if checkpoints else None
        # 只有“进行中”的检查点才阻止显示已恢复；“已失败”的检查点已经回到可恢复状态，
        # 后续成功检查点或恢复可以覆盖它。
        in_flight = [cp for cp in checkpoints if not cp.get("completed") and cp.get("status") == "进行中"]
        stream_row = store.find(STREAM_MODULE, int(job.get("job_id", 0)))
        recovery = job.get("recovery", {})
        # 未完成检查点不能显示成已恢复
        if in_flight and recovery.get("status") == "已恢复":
            recovery_status = "可恢复"
        else:
            recovery_status = recovery.get("status", "未发起")
        recovered = bool(recovery_status == "已恢复" and last and last.get("completed") and not in_flight)
        completed_checkpoints = [cp for cp in checkpoints if cp.get("completed")]
        previous_success = completed_checkpoints[-2] if len(completed_checkpoints) >= 2 else None
        if stream_row is not None:
            stream_row.update(
                {
                    "已确认水位": confirmed,
                    "检查点间隔": topic.get("checkpoint_interval", DEFAULT_CHECKPOINT_INTERVAL),
                    "最后成功检查点": last.get("checkpoint_id") if last else "—",
                    "上一次成功检查点": previous_success.get("checkpoint_id") if previous_success else "—",
                    "上一次成功水位": int(previous_success.get("watermark", 0)) if previous_success else 0,
                    "恢复状态": "已恢复" if recovered else recovery_status,
                    "剩余待处理事件": remaining,
                    "输出重复写入": 0,
                }
            )
        for row in store.rows(BACKFILL_MODULE):
            if str(row.get("关联流作业") or "") != str(job.get("job_no")):
                continue
            end = _parse_int(row.get("回填截止"))
            row.update(
                {
                    "回填水位": confirmed,
                    "同步检查点": last.get("checkpoint_id") if last else "—",
                    "待回填事件": max(end - confirmed, 0) if end is not None else 0,
                    "同步状态": "已同步",
                    "流作业恢复状态": "已恢复" if recovered else recovery_status,
                }
            )

    def sync_all(self) -> None:
        """把持久化检查点事实同步到各业务面列表，服务重启后页面读到的是恢复后的进度。"""
        with self._lock:
            data = self._load()
            for row in store.rows(STREAM_MODULE):
                job_no = str(row.get("作业编号") or "").strip()
                input_topic = str(row.get("输入Topic") or "").strip()
                if not job_no or not input_topic:
                    continue
                topic = self._ensure_topic(data, input_topic, row)
                job = self._ensure_job(data, row, topic)
                self._sync_rows(job, topic, data)

    def _lease_owner(self, topic: dict[str, Any], job: dict[str, Any]) -> bool:
        """判断当前作业是否持有输入 Topic 的消费租约。"""
        owner = topic.get("active_job_no")
        if owner is None:
            # 空租约时由第一个推进检查点的作业取得
            topic["active_job_no"] = job.get("job_no")
            return True
        return str(owner) == str(job.get("job_no"))

    def _job_checkpoints(self, topic: dict[str, Any], job: dict[str, Any]) -> list[dict[str, Any]]:
        return [cp for cp in topic.get("checkpoints", []) if str(cp.get("job_no") or "") == str(job.get("job_no"))]

    def _progress(self, job: dict[str, Any], topic: dict[str, Any], data: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            self._sync_rows(job, topic, data)
        holder = str(topic.get("active_job_no") or "") == str(job.get("job_no")) or topic.get("active_job_no") is None
        last = topic.get("last_success_checkpoint") if holder else None
        confirmed = int(topic.get("confirmed_watermark", 0) or 0) if holder else 0
        interval = int(topic.get("checkpoint_interval", DEFAULT_CHECKPOINT_INTERVAL) or DEFAULT_CHECKPOINT_INTERVAL)
        total = int(topic.get("input_total_events", 0) or 0)
        remaining = max(total - confirmed, 0)
        checkpoints = self._job_checkpoints(topic, job) if holder else []
        completed = [cp for cp in checkpoints if cp.get("completed")]
        latest = checkpoints[-1] if checkpoints else last
        incomplete = [cp for cp in checkpoints if not cp.get("completed")]
        in_flight = [cp for cp in incomplete if cp.get("status") == "进行中"]
        if latest and latest.get("completed"):
            previous_success = completed[-2] if len(completed) >= 2 else None
        else:
            previous_success = last
        output_topic = str(job.get("output_topic") or "")
        receipt = self._receipt_view(data, output_topic, str(job.get("job_no") or ""), str(job.get("input_topic") or ""))
        recovery = dict(job.get("recovery", {}))
        recovered = bool(recovery.get("status") == "已恢复" and last and last.get("completed") and not in_flight)
        if not recovered and recovery.get("status") == "已恢复":
            recovery["status"] = "可恢复"
        backfill_rows = [
            {
                "作业编号": row.get("作业编号"),
                "目标表名": row.get("目标表名"),
                "回填水位": row.get("回填水位"),
                "同步检查点": row.get("同步检查点"),
                "待回填事件": row.get("待回填事件"),
                "同步状态": row.get("同步状态"),
            }
            for row in store.rows(BACKFILL_MODULE)
            if str(row.get("关联流作业") or "") == str(job.get("job_no"))
        ]
        progress = {
            "作业编号": job.get("job_no"),
            "输入Topic": topic.get("input_topic"),
            "输出Topic": output_topic,
            "恢复状态": recovery.get("status", "未发起"),
            "恢复实例": job.get("active_instance") or recovery.get("instance_id"),
            "已恢复": recovered,
            "最后成功水位": confirmed,
            "检查点间隔": interval,
            "剩余待处理事件": remaining,
            "输入总量": total,
            "输入速率": int(topic.get("input_rate", DEFAULT_INPUT_RATE) or DEFAULT_INPUT_RATE),
            "本次检查点": _checkpoint_summary(latest),
            "上一次成功检查点": _checkpoint_summary(previous_success),
            "最后成功检查点": _checkpoint_summary(last),
            "未完成检查点": [_checkpoint_summary(cp) for cp in incomplete],
            "进行中检查点": [_checkpoint_summary(cp) for cp in in_flight],
            "输出回执": receipt,
            "回填台账": backfill_rows,
            "检查点记录": [_checkpoint_summary(cp) for cp in checkpoints],
            "恢复实例记录": [dict(item) for item in job.get("instances", [])],
            # 英文别名，便于接口调用方直接取值
            "job_no": job.get("job_no"),
            "recovery_status": recovery.get("status", "未发起"),
            "last_success_watermark": confirmed,
            "checkpoint_interval": interval,
            "remaining_events": remaining,
            "instance_id": job.get("active_instance") or recovery.get("instance_id"),
            "current_checkpoint": _checkpoint_summary(latest),
            "previous_success_checkpoint": _checkpoint_summary(previous_success),
            "output_receipt": receipt,
            "backfill_watermark": backfill_rows[0]["回填水位"] if backfill_rows else None,
            "recovered": recovered,
        }
        return progress

    def _receipt_view(self, data: dict[str, Any], output_topic: str, job_no: str, input_topic: str) -> dict[str, Any]:
        receipt = data.get("receipts", {}).get(output_topic)
        if not receipt:
            return {
                "输出Topic": output_topic,
                "回执水位": 0,
                "写入区间数": 0,
                "重复写入": 0,
                "已去重事件": 0,
                "最新回执": None,
                "与检查点一致": True,
            }
        ranges = [
            item
            for item in receipt.get("written_ranges", [])
            if str(item.get("job_no") or "") == job_no and str(item.get("input_topic") or "") == input_topic
        ]
        ranges.sort(key=lambda item: int(item.get("start", 0)))
        latest = ranges[-1] if ranges else None
        return {
            "输出Topic": output_topic,
            "回执水位": int(latest.get("end", 0)) if latest else 0,
            "写入区间数": len(ranges),
            "写入区间": [{"start": int(item.get("start", 0)), "end": int(item.get("end", 0)), "checkpoint_id": item.get("checkpoint_id")} for item in ranges],
            "写入事件数": sum(int(item.get("end", 0)) - int(item.get("start", 0)) + 1 for item in ranges),
            "重复写入": 0,
            "已去重事件": int(receipt.get("deduplicated_events", 0) or 0),
            "最新回执": dict(latest) if latest else None,
            "与检查点一致": True,
        }

    def progress_by_job_id(self, job_id: int) -> dict[str, Any] | None:
        row, job, data = self._resolve(job_id=job_id)
        if row is None or job is None:
            return None
        with self._lock:
            topic = data["topics"][job["input_topic"]]
            return self._progress(job, topic, data)

    def progress_by_job_no(self, job_no: str) -> dict[str, Any] | None:
        job_no = str(job_no or "").strip()
        if not job_no:
            return None
        row, job, data = self._resolve(job_no=job_no)
        if row is None or job is None:
            return None
        with self._lock:
            topic = data["topics"][job["input_topic"]]
            return self._progress(job, topic, data)

    def recovery_records(self, job_no: str | None = None) -> dict[str, Any]:
        if job_no:
            progress = self.progress_by_job_no(job_no)
            if progress is None:
                return {"ok": False, "message": f"作业 {job_no} 不存在或没有检查点", "items": [], "total": 0}
            return {
                "ok": True,
                "message": "已定位到恢复记录",
                "作业编号": job_no,
                "最后成功水位": progress["最后成功水位"],
                "检查点间隔": progress["检查点间隔"],
                "剩余待处理事件": progress["剩余待处理事件"],
                "本次检查点": progress["本次检查点"],
                "上一次成功检查点": progress["上一次成功检查点"],
                "检查点记录": progress["检查点记录"],
                "恢复实例记录": progress["恢复实例记录"],
                "recovery": progress,
                "items": progress["恢复实例记录"],
                "total": len(progress["恢复实例记录"]),
            }
        items: list[dict[str, Any]] = []
        for row in store.rows(STREAM_MODULE):
            no = str(row.get("作业编号") or "")
            progress = self.progress_by_job_no(no) if no else None
            if progress is None:
                continue
            items.append(
                {
                    "作业编号": no,
                    "输入Topic": progress["输入Topic"],
                    "恢复状态": progress["恢复状态"],
                    "最后成功水位": progress["最后成功水位"],
                    "检查点间隔": progress["检查点间隔"],
                    "剩余待处理事件": progress["剩余待处理事件"],
                    "本次检查点": progress["本次检查点"],
                    "上一次成功检查点": progress["上一次成功检查点"],
                }
            )
        return {"ok": True, "message": "恢复记录已读取", "items": items, "total": len(items)}

    # ------------------------------------------------------------------ 恢复
    def request_recovery(self, job_id: int, values: dict[str, Any]) -> tuple[bool, str, dict[str, Any] | None]:
        row, job, data = self._resolve(job_id=job_id)
        if row is None or job is None:
            return False, f"流处理作业 {job_id} 不存在或已归档", None
        with self._lock:
            topic = data["topics"][job["input_topic"]]
            progress_before = self._progress(job, topic, data)
            if not self._lease_owner(topic, job):
                return False, f"输入 Topic {topic.get('input_topic')} 当前由作业 {topic.get('active_job_no')} 持有消费租约，本作业不能推进水位", progress_before
            last = topic.get("last_success_checkpoint")
            if not last or not last.get("completed"):
                return False, "没有可用的成功检查点，无法恢复", progress_before

            interval_value = values.get("checkpoint_interval")
            interval: int | None = None
            if interval_value not in (None, ""):
                interval = _parse_interval(interval_value, fallback=-1)
                if interval is None:
                    return False, f"检查点间隔「{interval_value}」非法，必须为不超过 {MAX_CHECKPOINT_INTERVAL} 的正整数", progress_before

            watermark_value = values.get("watermark")
            requested_watermark: int | None = None
            if watermark_value not in (None, ""):
                requested_watermark = _parse_int(watermark_value)
                if requested_watermark is None or requested_watermark < 0:
                    return False, f"恢复水位「{watermark_value}」非法，必须是大于等于 0 的整数", progress_before
                if requested_watermark > int(last.get("watermark", 0)):
                    return False, f"恢复水位 {requested_watermark} 高于最后成功水位 {last.get('watermark')}，已拒绝", progress_before

            instance_id = str(values.get("instance_id") or "").strip()
            active = job.get("active_instance")
            if active:
                existing = next((item for item in job.get("instances", []) if item.get("instance_id") == active), None)
                if instance_id and existing and instance_id == active:
                    return True, f"恢复实例 {active} 已生效，重复提交已合并", progress_before
                return False, f"恢复实例 {active} 正在生效，本次请求未创建新实例", progress_before

            if instance_id:
                existing = next((item for item in job.get("instances", []) if item.get("instance_id") == instance_id), None)
                if existing is not None:
                    if existing.get("completed"):
                        return True, f"恢复实例 {instance_id} 已生效，重复提交已合并", progress_before
                    return False, f"恢复实例 {instance_id} 正在生效，本次请求未创建新实例", progress_before

            # 同一目标检查点上的重复恢复请求在短时间内合并为一个实例：
            # 两个请求同时到达（或重启探测重复投递）时只允许一个实例生效。
            latest_instance = job.get("instances", [])[-1] if job.get("instances") else None
            if latest_instance is not None and not bool(values.get("force")):
                same_target = (
                    latest_instance.get("from_checkpoint") == last.get("checkpoint_id")
                    and latest_instance.get("requested_watermark") == requested_watermark
                    and (interval is None or latest_instance.get("requested_interval") == interval)
                )
                if same_target and latest_instance.get("status") == "恢复中":
                    return False, f"恢复实例 {latest_instance.get('instance_id')} 正在生效，本次请求未创建新实例", progress_before
                if same_target and latest_instance.get("completed"):
                    since = latest_instance.get("completed_at")
                    if since and (datetime.now(timezone.utc) - datetime.fromisoformat(since)).total_seconds() <= DEDUP_WINDOW_SECONDS:
                        return True, f"恢复实例 {latest_instance.get('instance_id')} 已生效，重复提交已合并", progress_before

            if not instance_id:
                sequence = int(job.get("next_instance_sequence", 1) or 1)
                instance_id = f"RC-{job.get('job_no')}-{sequence:06d}"
                job["next_instance_sequence"] = sequence + 1

            requested_at = _now()
            resume_watermark = int(last.get("watermark", 0))
            remaining = max(int(topic.get("input_total_events", 0) or 0) - resume_watermark, 0)
            instance = {
                "instance_id": instance_id,
                "status": "恢复中",
                "completed": False,
                "requested_at": requested_at,
                "completed_at": None,
                "from_checkpoint": last.get("checkpoint_id"),
                "from_watermark": resume_watermark,
                "requested_watermark": requested_watermark,
                "requested_interval": interval if interval is not None else topic.get("checkpoint_interval"),
                "remaining_events": remaining,
                "message": "正在从最后成功检查点恢复",
            }
            job["instances"].append(instance)
            job["active_instance"] = instance_id
            job["recovery"] = {
                "instance_id": instance_id,
                "status": "恢复中",
                "completed": False,
                "requested_at": requested_at,
                "completed_at": None,
                "message": "正在从最后成功检查点恢复",
            }
            if row is not None:
                row["status"] = "恢复中"
                row["pending"] = True

            if bool(values.get("simulate_failure")):
                instance.update(
                    {
                        "status": "恢复失败",
                        "completed": False,
                        "completed_at": _now(),
                        "message": "恢复过程中断，已退回可恢复状态",
                    }
                )
                job["recovery"].update(
                    {
                        "status": "恢复失败",
                        "completed": False,
                        "completed_at": _now(),
                        "message": "恢复过程中断，已退回可恢复状态",
                    }
                )
                job["active_instance"] = None
                if row is not None:
                    row["status"] = "可恢复"
                    row["pending"] = True
                    row["abnormal"] = True
                self._append_audit("检查点恢复失败", str(job.get("job_no")), f"实例 {instance_id} 恢复失败，保留最后成功检查点 {last.get('checkpoint_id')}", 0, "可重新发起恢复")
                self._persist(data)
                progress_before = self._progress(job, topic, data)
                return False, "恢复失败，作业已回到可恢复状态", progress_before

            if interval is not None:
                topic["checkpoint_interval"] = interval
            instance.update(
                {
                    "status": "已恢复",
                    "completed": True,
                    "completed_at": _now(),
                    "message": f"已从检查点 {last.get('checkpoint_id')} 恢复",
                }
            )
            job["recovery"] = {
                "instance_id": instance_id,
                "status": "已恢复",
                "completed": True,
                "requested_at": requested_at,
                "completed_at": instance["completed_at"],
                "message": instance["message"],
            }
            # 恢复成功后释放实例锁，后续新的失败场景仍能再次发起恢复
            job["active_instance"] = None
            if row is not None:
                row["status"] = "已恢复"
                row["pending"] = False
                row["abnormal"] = False
            self._append_audit(
                "检查点恢复",
                str(job.get("job_no")),
                f"实例 {instance_id} 从 {last.get('checkpoint_id')}（水位 {resume_watermark}）恢复，剩余 {remaining} 条",
                0,
                f"可回滚至 {last.get('checkpoint_id')}",
            )
            self._persist(data)
            progress = self._progress(job, topic, data)
            return True, f"已从最后成功检查点 {last.get('checkpoint_id')} 恢复，剩余待处理事件 {remaining}", progress

    # ------------------------------------------------------------------ 检查点写入
    def commit_checkpoint(self, job_id: int, values: dict[str, Any]) -> tuple[bool, str, dict[str, Any] | None]:
        row, job, data = self._resolve(job_id=job_id)
        if row is None or job is None:
            return False, f"流处理作业 {job_id} 不存在或已归档", None
        with self._lock:
            topic = data["topics"][job["input_topic"]]
            progress_before = self._progress(job, topic, data)
            if not self._lease_owner(topic, job):
                return False, f"输入 Topic {topic.get('input_topic')} 当前由作业 {topic.get('active_job_no')} 持有消费租约，本作业不能写入检查点", progress_before
            confirmed = int(topic.get("confirmed_watermark", 0) or 0)
            total = int(topic.get("input_total_events", 0) or 0)
            checkpoint_id = str(values.get("checkpoint_id") or "").strip()

            # 同一个检查点重复提交时按幂等处理，不再重复推进水位
            if checkpoint_id:
                existing = next(
                    (cp for cp in topic.get("checkpoints", []) if cp.get("checkpoint_id") == checkpoint_id and cp.get("completed")),
                    None,
                )
                if existing is not None:
                    message = f"检查点 {checkpoint_id} 已确认，重复提交已合并"
                    return True, message, progress_before

            watermark = _parse_int(values.get("watermark"))
            if watermark is None:
                raw = values.get("watermark")
                return False, f"水位「{raw}」非法，必须是整数", progress_before
            if watermark < 0:
                return False, f"水位 {watermark} 非法，不能为负数", progress_before
            if watermark <= confirmed:
                return False, f"水位 {watermark} 未高于最后成功水位 {confirmed}，冲突时以最后成功检查点为准，已拒绝", progress_before
            if total > 0 and watermark > total:
                return False, f"水位 {watermark} 超出输入 Topic 可确认范围（上限 {total}），已拒绝", progress_before

            interval_value = values.get("checkpoint_interval")
            interval = int(topic.get("checkpoint_interval", DEFAULT_CHECKPOINT_INTERVAL) or DEFAULT_CHECKPOINT_INTERVAL)
            if interval_value not in (None, ""):
                parsed_interval = _parse_interval(interval_value, fallback=-1)  # type: ignore[arg-type]
                if parsed_interval is None:
                    return False, f"检查点间隔「{interval_value}」非法，必须为不超过 {MAX_CHECKPOINT_INTERVAL} 的正整数", progress_before
                interval = parsed_interval

            sequence = int(topic.get("next_sequence", 1) or 1)
            if not checkpoint_id:
                checkpoint_id = f"CP-{job.get('job_no')}-{sequence:06d}"
            if any(cp.get("checkpoint_id") == checkpoint_id for cp in topic.get("checkpoints", [])):
                return False, f"检查点编号 {checkpoint_id} 已被占用，未重复创建", progress_before

            output_topic = str(job.get("output_topic") or "").strip()
            from_watermark = confirmed
            event_count = watermark - from_watermark if from_watermark > 0 else watermark
            output_range = [from_watermark + 1, watermark] if confirmed > 0 else [1, watermark]
            now = _now()
            checkpoint = {
                "checkpoint_id": checkpoint_id,
                "watermark": watermark,
                "from_watermark": from_watermark,
                "event_count": event_count,
                "output_range": output_range,
                "status": "已完成",
                "completed": True,
                "created_at": now,
                "completed_at": now,
                "job_no": str(job.get("job_no")),
            }
            attempt = {
                "checkpoint_id": checkpoint_id,
                "watermark": watermark,
                "from_watermark": from_watermark,
                "event_count": event_count,
                "output_range": output_range,
                "status": "进行中",
                "completed": False,
                "created_at": now,
                "completed_at": None,
                "job_no": str(job.get("job_no")),
            }

            state_before = copy.deepcopy(data)
            snapshots = {
                module: [dict(item) for item in store.rows(module)]
                for module in (STREAM_MODULE, BACKFILL_MODULE, AUDIT_MODULE)
            }
            try:
                if not output_topic:
                    raise RuntimeError("输出 Topic 未配置")

                # 输出回执按编号区间幂等：重复区间不再写第二次
                receipt, written, skipped = self._write_output_receipt(
                    data, output_topic, job, topic, checkpoint
                )
                topic["next_sequence"] = sequence + 1
                topic["checkpoint_interval"] = interval
                topic["checkpoints"].append(checkpoint)
                topic["last_success_checkpoint"] = checkpoint
                if bool(values.get("simulate_output_failure")):
                    raise RuntimeError("输出 Topic 写入失败")

                topic["confirmed_watermark"] = watermark
                previous = [cp for cp in topic["checkpoints"] if cp.get("completed") and cp is not checkpoint]
                previous_checkpoint = previous[-1] if previous else None
                remaining = max(total - watermark, 0)

                row.update(
                    {
                        "status": "运行中",
                        "pending": False,
                        "abnormal": False,
                        "已确认水位": watermark,
                        "检查点间隔": interval,
                        "最后成功检查点": checkpoint_id,
                        "上一次成功检查点": previous_checkpoint.get("checkpoint_id") if previous_checkpoint else "—",
                        "恢复状态": "已恢复",
                        "剩余待处理事件": remaining,
                        "输出重复写入": 0,
                    }
                )
                job["active_instance"] = None
                job["recovery"] = {
                    "instance_id": job.get("recovery", {}).get("instance_id"),
                    "status": "已恢复",
                    "completed": True,
                    "requested_at": job.get("recovery", {}).get("requested_at"),
                    "completed_at": now,
                    "message": f"检查点 {checkpoint_id} 已确认",
                }
                self._sync_backfill(str(job.get("job_no")), watermark, checkpoint_id, "已恢复")
                self._append_audit(
                    "流作业检查点",
                    str(job.get("job_no")),
                    f"检查点 {checkpoint_id} 水位 {from_watermark}→{watermark}，写入 {written} 条、去重 {skipped} 条",
                    written,
                    f"可回滚至 {previous_checkpoint.get('checkpoint_id') if previous_checkpoint else '起始水位'}",
                )
                self._persist(data)
            except Exception as error:  # 输出失败或落盘失败：整体回滚到可恢复状态
                job_no = str(job.get("job_no"))
                self._restore_rows(snapshots)
                data.clear()
                data.update(state_before)
                job = data["jobs"][job_no]
                topic = data["topics"][job["input_topic"]]
                failed = dict(attempt)
                failed.update(
                    {
                        "status": "已失败",
                        "completed": False,
                        "completed_at": _now(),
                        "failure_reason": str(error),
                    }
                )
                topic["checkpoints"].append(failed)
                topic["next_sequence"] = sequence + 1
                if topic.get("last_success_checkpoint"):
                    topic["confirmed_watermark"] = int(topic["last_success_checkpoint"].get("watermark", 0))
                job["active_instance"] = None
                job["recovery"] = {
                    "instance_id": job.get("recovery", {}).get("instance_id"),
                    "status": "恢复失败",
                    "completed": False,
                    "requested_at": job.get("recovery", {}).get("requested_at"),
                    "completed_at": _now(),
                    "message": f"检查点 {checkpoint_id} 写入失败：{error}",
                }
                if row is not None:
                    row["status"] = "可恢复"
                    row["pending"] = True
                    row["abnormal"] = True
                    row["恢复状态"] = "恢复失败"
                self._sync_backfill(job_no, int(topic.get("confirmed_watermark", 0)), str((topic.get("last_success_checkpoint") or {}).get("checkpoint_id") or "—"), "恢复失败")
                self._append_audit(
                    "检查点写入失败",
                    job_no,
                    f"检查点 {checkpoint_id} 水位 {from_watermark}→{watermark} 写入失败：{error}，已保持最后成功水位 {topic.get('confirmed_watermark')}",
                    0,
                    f"可回滚至 {(topic.get('last_success_checkpoint') or {}).get('checkpoint_id') or '起始水位'}",
                )
                try:
                    self._persist(data)
                except OSError:
                    pass
                progress = self._progress(job, topic, data)
                return False, f"检查点未完成：{error}；作业已回到可恢复状态，水位保持在 {topic.get('confirmed_watermark')}", progress

            progress = self._progress(job, topic, data)
            return True, f"检查点 {checkpoint_id} 已确认，水位推进到 {watermark}", progress

    def _write_output_receipt(
        self,
        data: dict[str, Any],
        output_topic: str,
        job: dict[str, Any],
        topic: dict[str, Any],
        checkpoint: dict[str, Any],
    ) -> tuple[dict[str, Any], int, int]:
        receipt = data["receipts"].setdefault(
            output_topic,
            {"output_topic": output_topic, "written_ranges": [], "deduplicated_events": 0},
        )
        start, end = checkpoint["output_range"]
        # 计算尚未写入的编号片段，已写入的编号绝不重复提交
        missing: list[tuple[int, int]] = []
        cursor = start
        for item in sorted(receipt.get("written_ranges", []), key=lambda row: int(row.get("start", 0))):
            left, right = int(item.get("start", 0)), int(item.get("end", 0))
            if right < cursor:
                continue
            if left > end:
                break
            if left > cursor:
                missing.append((cursor, min(left - 1, end)))
            cursor = max(cursor, right + 1)
            if cursor > end:
                break
        if cursor <= end:
            missing.append((cursor, end))
        written = sum(right - left + 1 for left, right in missing)
        skipped = (end - start + 1) - written
        recorded_at = _now()
        for left, right in missing:
            receipt["written_ranges"].append(
                {
                    "start": left,
                    "end": right,
                    "checkpoint_id": checkpoint["checkpoint_id"],
                    "job_no": str(job.get("job_no")),
                    "input_topic": topic.get("input_topic"),
                    "recorded_at": recorded_at,
                }
            )
        receipt["written_ranges"].sort(key=lambda item: int(item.get("start", 0)))
        receipt["deduplicated_events"] = int(receipt.get("deduplicated_events", 0) or 0) + skipped
        receipt["last_receipt"] = {
            "checkpoint_id": checkpoint["checkpoint_id"],
            "watermark": checkpoint["watermark"],
            "written": written,
            "skipped": skipped,
            "recorded_at": recorded_at,
        }
        return receipt, written, skipped

    def _restore_rows(self, snapshots: dict[str, list[dict[str, Any]]]) -> None:
        for module, rows in snapshots.items():
            live = store.rows(module)
            live.clear()
            live.extend(dict(item) for item in rows)

    def _sync_backfill(self, job_no: str, watermark: int, checkpoint_id: str, recovery_status: str) -> None:
        for row in store.rows(BACKFILL_MODULE):
            if str(row.get("关联流作业") or "") != job_no:
                continue
            end = _parse_int(row.get("回填截止"))
            row.update(
                {
                    "回填水位": watermark,
                    "同步检查点": checkpoint_id,
                    "待回填事件": max(end - watermark, 0) if end is not None else 0,
                    "同步状态": "已同步" if recovery_status == "已恢复" else "待同步",
                    "流作业恢复状态": recovery_status,
                }
            )

    def _append_audit(self, action: str, job_no: str, detail: str, affected: int, rollback: str) -> None:
        rows = store.rows(AUDIT_MODULE)
        entry_id = max((int(row.get("id", 0)) for row in rows), default=0) + 1
        rows.append(
            {
                "id": entry_id,
                "status": "已归档",
                "pending": False,
                "abnormal": False,
                "日志编号": f"AUDI-{entry_id:04d}",
                "操作类型": action,
                "操作对象": job_no,
                "操作详情": detail,
                "操作来源": "流式处理检查点",
                "执行时间": _now(),
                "影响行数": affected,
                "回滚能力": rollback,
            }
        )


checkpoint_service = CheckpointService(
    Path(settings.checkpoint_state_path)
    if settings.checkpoint_state_path
    else Path(__file__).resolve().parents[2] / "var" / "checkpoints.json"
)
