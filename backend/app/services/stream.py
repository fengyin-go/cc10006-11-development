"""流式处理业务规则：状态流转、字段校验与筛选口径都收在这里。"""
from __future__ import annotations

from typing import Any

from app.store import store

MODULE = "stream"
REQUIRED_FIELDS = ["作业编号", "输入Topic", "处理逻辑"]
# 登记时允许一并写入的执行参数：检查点间隔、输出 Topic、输入总量等都参与恢复计算
OPTIONAL_FIELDS = ["窗口类型", "窗口时长秒", "水位延迟", "检查点间隔", "输出Topic", "输入总量", "输入速率"]
STATUS_ORDER = ["草稿", "待校验", "运行中", "已停止", "已恢复", "恢复中", "可恢复"]
ACTION_RULES = {"提交校验": "待校验", "启动作业": "运行中", "停止作业": "已停止"}
NEGATIVE_ACTIONS = []


class StreamService:
    def list_entries(
        self,
        *,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = store.rows(MODULE)
        if keyword:
            rows = [row for row in rows if keyword in str(row.get("作业编号", ""))]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = max(page - 1, 0) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        job_no = str(values.get("作业编号")).strip()
        if any(str(row.get("作业编号")) == job_no for row in store.rows(MODULE)):
            return None, ["作业编号已被占用，请换一个编号再登记"]
        rows = store.rows(MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in REQUIRED_FIELDS})
        for field in OPTIONAL_FIELDS:
            if values.get(field) not in (None, ""):
                entry[field] = values.get(field)
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        entry["恢复状态"] = "未发起"
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"流处理作业 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于流式处理可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        if action == "启动作业":
            entry["恢复状态"] = "可恢复"
        return entry, f"流处理作业已{action}"
