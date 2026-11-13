"""流处理检查点恢复的端到端回归脚本（仅依赖标准库，可重复运行）。

用法：先在 backend 目录起好服务，然后执行
    .venv/bin/python scripts/verify_checkpoint_recovery.py [--base-url http://127.0.0.1:8000]

脚本会验证：
1. 恢复面板字段（最后成功水位、检查点间隔、剩余待处理事件）齐全；
2. 并发恢复只保留一个实例；
3. 非法水位被拒绝；
4. 检查点写入失败时回滚到可恢复状态，输出回执不前进；
5. 输出编号在重试/重启后不会重复写入。
"""
from __future__ import annotations

import argparse
import json
import sys
import threading
import urllib.error
import urllib.request

TIMEOUT = 10


def call(base: str, method: str, path: str, body: dict | None = None) -> tuple[int, dict]:
    data = json.dumps(body, ensure_ascii=False).encode("utf-8") if body is not None else None
    request = urllib.request.Request(
        f"{base}{path}",
        data=data,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=TIMEOUT) as response:
            return response.status, json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as error:
        return error.code, json.loads(error.read().decode("utf-8"))


def expect(condition: bool, label: str, failures: list[str]) -> None:
    print(("PASS " if condition else "FAIL ") + label)
    if not condition:
        failures.append(label)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--job-id", type=int, default=3, help="带检查点示例数据的流作业 id")
    args = parser.parse_args()
    base = args.base_url.rstrip("/")
    job_id = args.job_id
    failures: list[str] = []

    status, progress = call(base, "GET", f"/api/stream/recovery/{job_id}")
    expect(status == 200, "恢复面板可读取", failures)
    for field in ("最后成功水位", "检查点间隔", "剩余待处理事件", "本次检查点", "上一次成功检查点", "输出回执", "回填台账"):
        expect(field in progress, f"恢复面板包含「{field}」", failures)
    expect(progress.get("已恢复") is True, "示例作业显示为已恢复", failures)

    # 并发恢复：两个请求同时到达
    results: list[tuple[int, dict]] = []

    def request_recovery() -> None:
        results.append(call(base, "POST", f"/api/stream/{job_id}/actions", {"values": {"action": "恢复作业"}}))

    threads = [threading.Thread(target=request_recovery) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    instances = progress.get("恢复实例记录", [])
    after = call(base, "GET", f"/api/stream/recovery/{job_id}")[1]
    expect(
        len(after.get("恢复实例记录", [])) - len(instances) == 1,
        "两个并发恢复请求只新增一个实例",
        failures,
    )
    expect(any(result[1].get("ok") for result in results), "至少一个恢复请求成功", failures)
    instance_ids = {
        result[1].get("entry", {}).get("恢复进度", {}).get("instance_id")
        for result in results
    }
    expect(len(instance_ids) == 1, "并发请求指向同一个恢复实例", failures)

    # 非法水位
    bad = call(base, "POST", f"/api/stream/{job_id}/actions", {"values": {"action": "提交检查点", "watermark": "abc"}})[1]
    expect(bad.get("ok") is False and "非法" in bad.get("message", ""), "非整数水位被拒绝", failures)
    negative = call(base, "POST", f"/api/stream/{job_id}/actions", {"values": {"action": "提交检查点", "watermark": -5}})[1]
    expect(negative.get("ok") is False and "不能为负数" in negative.get("message", ""), "负水位被拒绝", failures)
    lower = call(
        base,
        "POST",
        f"/api/stream/{job_id}/actions",
        {"values": {"action": "提交检查点", "watermark": max(int(after["最后成功水位"]) - 1, 0)}},
    )[1]
    expect(lower.get("ok") is False and "未高于最后成功水位" in lower.get("message", ""), "不推进水位的提交被拒绝", failures)
    bad_interval = call(
        base,
        "POST",
        f"/api/stream/{job_id}/actions",
        {"values": {"action": "提交检查点", "watermark": int(after["最后成功水位"]) + 1, "checkpoint_interval": 0}},
    )[1]
    expect(bad_interval.get("ok") is False and "检查点间隔" in bad_interval.get("message", ""), "非法检查点间隔被拒绝", failures)

    # 输出失败回滚
    before = call(base, "GET", f"/api/stream/recovery/{job_id}")[1]
    target = int(before["最后成功水位"]) + 500
    failed = call(
        base,
        "POST",
        f"/api/stream/{job_id}/actions",
        {"values": {"action": "提交检查点", "watermark": target, "simulate_output_failure": True}},
    )[1]
    expect(failed.get("ok") is False, "输出写入失败时检查点提交失败", failures)
    after_failure = call(base, "GET", f"/api/stream/recovery/{job_id}")[1]
    expect(after_failure["最后成功水位"] == before["最后成功水位"], "失败后水位回到最后成功检查点", failures)
    expect(after_failure["输出回执"]["回执水位"] == before["输出回执"]["回执水位"], "失败后输出回执未前进", failures)
    expect(after_failure["恢复状态"] != "已恢复", "未完成检查点不显示成已恢复", failures)

    # 重试同一区间，输出不重复
    retried = call(
        base,
        "POST",
        f"/api/stream/{job_id}/actions",
        {"values": {"action": "提交检查点", "watermark": target}},
    )[1]
    expect(retried.get("ok") is True, "失败后可重新提交同一区间检查点", failures)
    retry_progress = retried.get("entry", {}).get("恢复进度", {})
    expect(retry_progress.get("输出回执", {}).get("重复写入") == 0, "重试后输出回执无重复写入", failures)
    expect(retry_progress.get("最后成功水位") == target, "重试后水位推进到目标值", failures)

    # 三方视图一致
    stream_entry = call(base, "GET", f"/api/stream/{job_id}")[1]
    backfills = call(base, "GET", "/api/backfill?size=200")[1]
    related = [row for row in backfills.get("items", []) if row.get("关联流作业") == stream_entry.get("作业编号")]
    expect(stream_entry.get("已确认水位") == target, "流作业列表水位与检查点一致", failures)
    expect(all(int(row.get("回填水位", 0)) == target for row in related), "回填台账水位同步推进", failures)
    expect(all(row.get("同步检查点") == retry_progress["最后成功检查点"]["检查点编号"] for row in related), "回填台账检查点一致", failures)

    records = call(base, "GET", f"/api/stream/recovery?job_no={stream_entry.get('作业编号')}")[1]
    expect(records.get("ok") is True, "按作业编号可定位恢复记录", failures)
    expect(records.get("本次检查点") is not None, "恢复记录包含本次检查点", failures)
    expect(records.get("上一次成功检查点") is not None, "恢复记录包含上一次成功检查点", failures)

    # 输入 Topic 租约：复用同一输入 Topic 的新作业不能推进水位，也不会看到别人的检查点
    created = call(
        base,
        "POST",
        "/api/stream",
        {
            "values": {
                "作业编号": f"STRE-VERIFY-{target}",
                "输入Topic": before["输入Topic"],
                "处理逻辑": "回归脚本创建的作业",
                "输出Topic": before["输出Topic"],
            }
        },
    )
    if created[1].get("ok"):
        new_id = created[1]["entry"]["id"]
        rejected = call(
            base,
            "POST",
            f"/api/stream/{new_id}/actions",
            {"values": {"action": "提交检查点", "watermark": target + 100}},
        )[1]
        expect("租约" in rejected.get("message", ""), "复用输入 Topic 的作业被租约拦截", failures)
        new_progress = call(base, "GET", f"/api/stream/recovery/{new_id}")[1]
        expect(new_progress.get("最后成功水位") == 0, "非租约作业看不到别人的水位", failures)
        expect(new_progress.get("检查点记录") == [], "非租约作业看不到别人的检查点", failures)

    print("-" * 40)
    if failures:
        print(f"{len(failures)} 项未通过：")
        for item in failures:
            print(f"  - {item}")
        return 1
    print("全部检查通过")
    return 0


if __name__ == "__main__":
    sys.exit(main())
