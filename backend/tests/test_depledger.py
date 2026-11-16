"""依赖就绪账本的单元测试：用 stdlib unittest，不引入额外依赖。

覆盖需求里的关键口径：
- 跨日偏移、时区统一、同一上游多运行结果的水位版本裁决；
- 连续提交同一事件只生效一次（水位/等待/恢复三处幂等）；
- 迟到完成事件更正「已过期但尚未触发」的依赖并保留版本；
- 等待超时落入异常重跑清单且随重算幂等更新；
- 重跑从快照恢复等待队列与运行面状态，断点保留可核对；
- 水位与就绪快照同事务落库，失败整体回滚。
"""
from __future__ import annotations

import unittest
from datetime import datetime, timezone

from app.services import ledger_domain as dom
from app.services.depledger import DependencyLedgerService
from app.store import Store


class FreshStore(Store):
    """每个用例独立的空库，避免与 seed 数据相互污染。"""

    def __init__(self) -> None:
        super().__init__()
        for name in ("dep_watermark", "dep_wait_event", "dep_ledger", "dep_recovery_log"):
            self._tables[name] = []
        self._tables["dependency"] = []
        self._tables["rerun"] = []
        self._tables["runtime"] = []


def make_relation(store: Store, code: str, upstream: str, downstream: str,
                  offset: int = 0, timeout: int = 240) -> dict:
    row = {
        "id": store.next_id("dependency"), "status": "已生效", "pending": False,
        "abnormal": False, "关系编号": code, "上游作业": upstream, "下游作业": downstream,
        "依赖类型": "强依赖", "偏移量分钟": offset, "触发条件": "上游成功",
        "等待超时": timeout, "跳过标志": "否",
    }
    store.rows("dependency").append(row)
    return row


class TimezoneAndOffsetTest(unittest.TestCase):
    def test_timezone_normalized_to_utc(self) -> None:
        sh = dom.parse_dt("2026-11-16T02:10:00+08:00", "Asia/Shanghai")
        utc = dom.parse_dt("2026-11-15T18:10:00+00:00", "UTC")
        self.assertEqual(sh, utc)
        self.assertEqual(sh.tzinfo, timezone.utc)

    def test_naive_timestamp_uses_source_tz(self) -> None:
        dt = dom.parse_dt("2026-11-16T02:10:00", "Asia/Shanghai")
        self.assertEqual(dt, datetime(2026, 11, 15, 18, 10, tzinfo=timezone.utc))

    def test_cross_day_offset(self) -> None:
        from datetime import date
        self.assertEqual(
            dom.shift_biz_date(date(2026, 11, 16), -1440), date(2026, 11, 15)
        )
        self.assertEqual(
            dom.shift_biz_date(date(2026, 11, 15), 1440), date(2026, 11, 16)
        )

    def test_watermark_choice_late_event_wins_across_timezone(self) -> None:
        candidates = [
            {"id": 1, "事件序号": 1, "时区": "UTC",
             "事件时间": "2026-11-15T17:30:00+00:00", "上报时间": "2026-11-15T17:35:00+00:00"},
            {"id": 2, "事件序号": 2, "时区": "Asia/Shanghai",
             "事件时间": "2026-11-16T02:10:00+08:00", "上报时间": "2026-11-15T18:15:00+00:00"},
            {"id": 3, "事件序号": 1, "时区": "Asia/Shanghai",
             "事件时间": "2026-11-16T01:20:00+08:00", "上报时间": "2026-11-15T17:25:00+00:00"},
        ]
        chosen = dom.choose_watermark(candidates)
        self.assertEqual(chosen["id"], 2)  # 迟到的完成事件（UTC 口径最晚）胜出

    def test_watermark_choice_uses_seq_when_event_time_tied(self) -> None:
        candidates = [
            {"id": 1, "事件序号": 1, "时区": "UTC", "事件时间": "2026-11-15T17:30:00+00:00",
             "上报时间": "2026-11-15T17:30:00+00:00"},
            {"id": 2, "事件序号": 3, "时区": "UTC", "事件时间": "2026-11-15T17:30:00+00:00",
             "上报时间": "2026-11-15T17:30:00+00:00"},
        ]
        self.assertEqual(dom.choose_watermark(candidates)["id"], 2)


class LedgerFlowTest(unittest.TestCase):
    def setUp(self) -> None:
        self.store = FreshStore()
        self.svc = DependencyLedgerService(self.store)
        make_relation(self.store, "DEPE-T1", "up_a", "down_a", offset=0, timeout=240)
        make_relation(self.store, "DEPE-T2", "up_b", "down_b", offset=-1440, timeout=240)

    # 幂等：同一水位事件连续提交只生效一次
    def test_duplicate_watermark_ingested_once(self) -> None:
        payload = {"事件编号": "WM-1", "上游作业": "up_a", "业务日期": "2026-11-15",
                   "事件时间": "2026-11-16T01:00:00+08:00", "时区": "Asia/Shanghai"}
        row1, _, created1 = self.svc.ingest_watermark(dict(payload))
        row2, _, created2 = self.svc.ingest_watermark(dict(payload))
        self.assertTrue(created1)
        self.assertFalse(created2)
        self.assertEqual(row1["id"], row2["id"])
        self.assertEqual(len(self.store.rows("dep_watermark")), 1)

    # 水位满足触发条件才生成下游可执行结果，且记录来源水位
    def test_ready_only_when_trigger_met(self) -> None:
        self.svc.enqueue_wait({"等待编号": "W-1", "关系编号": "DEPE-T1",
                               "业务日期": "2026-11-15", "时区": "Asia/Shanghai"})
        snap = next(iter(self.store.rows("dep_ledger")))
        self.assertEqual(snap["就绪状态"], dom.STATE_WAITING)
        self.svc.ingest_watermark({"事件编号": "WM-1", "上游作业": "up_a",
                                   "业务日期": "2026-11-15",
                                   "事件时间": "2026-11-16T01:00:00+08:00",
                                   "时区": "Asia/Shanghai"})
        snap = next(iter(self.store.rows("dep_ledger")))
        self.assertEqual(snap["就绪状态"], dom.STATE_READY)
        self.assertEqual(snap["采用水位编号"], "WM-1")
        self.assertEqual(snap["偏移量分钟"], 0)

    # 迟到完成事件更正尚未触发的等待，保留版本与上一状态
    def test_late_event_corrects_waiting_with_version(self) -> None:
        self.svc.enqueue_wait({"等待编号": "W-2", "关系编号": "DEPE-T2",
                               "业务日期": "2026-11-16", "时区": "Asia/Shanghai"})
        snap = next(iter(self.store.rows("dep_ledger")))
        self.assertEqual(snap["就绪状态"], dom.STATE_WAITING)
        self.assertEqual(snap["上游批次日"], "2026-11-15")  # 跨日偏移已折算
        # 迟到的上游完成事件（up_b 的 11-15 批次）
        self.svc.ingest_watermark({"事件编号": "WM-2", "上游作业": "up_b",
                                   "业务日期": "2026-11-15",
                                   "事件时间": "2026-11-16T03:00:00+08:00",
                                   "时区": "Asia/Shanghai"})
        snap = next(iter(self.store.rows("dep_ledger")))
        self.assertEqual(snap["就绪状态"], dom.STATE_READY)
        self.assertEqual(snap["上一状态"], dom.STATE_WAITING)
        self.assertEqual(snap["版本号"], 2)

    # 重复重算不重复下发：状态不变不升版本、不新增快照
    def test_recompute_idempotent_snapshot(self) -> None:
        self.svc.enqueue_wait({"等待编号": "W-3", "关系编号": "DEPE-T1",
                               "业务日期": "2026-11-15", "时区": "Asia/Shanghai"})
        self.svc.ingest_watermark({"事件编号": "WM-3", "上游作业": "up_a",
                                   "业务日期": "2026-11-15",
                                   "事件时间": "2026-11-16T01:00:00+08:00",
                                   "时区": "Asia/Shanghai"})
        self.svc.recompute()
        self.svc.recompute()
        ledger = self.store.rows("dep_ledger")
        self.assertEqual(len(ledger), 1)
        # 入队时 v1（等待中），水位满足后合法演进到 v2（已就绪）；
        # 之后两次重算状态不变，版本不应再被抬高
        self.assertEqual(ledger[0]["版本号"], 2)

    # 等待超时落入异常重跑清单，再次重算只更新不重复建单
    def test_timeout_creates_single_rerun(self) -> None:
        make_relation(self.store, "DEPE-T3", "up_c", "down_c", offset=0, timeout=1)
        self.svc.enqueue_wait({"等待编号": "W-4", "关系编号": "DEPE-T3",
                               "业务日期": "2026-11-15", "时区": "UTC",
                               "入队时间": "2026-11-15T00:00:00+00:00"})
        snap = next(iter(self.store.rows("dep_ledger")))
        self.assertEqual(snap["就绪状态"], dom.STATE_TIMEOUT)
        self.assertIn("超过 1 分钟", snap["超时原因"])
        reruns = self.svc.list_rerun_candidates()
        self.assertEqual(len(reruns), 1)
        self.assertEqual(reruns[0]["来源快照编号"], snap["快照编号"])
        self.svc.recompute()
        self.assertEqual(len(self.svc.list_rerun_candidates()), 1)

    # 重跑恢复：等待队列、运行面状态恢复，断点保留，重复恢复幂等
    def test_recover_restores_queue_and_runtime_keeps_breakpoint(self) -> None:
        make_relation(self.store, "DEPE-T4", "up_d", "down_d", offset=0, timeout=1)
        self.store.rows("runtime").append({
            "id": 1, "status": "失败", "pending": True, "abnormal": True,
            "记录编号": "RUNT-X", "作业名称": "up_d",
        })
        self.svc.enqueue_wait({"等待编号": "W-5", "关系编号": "DEPE-T4",
                               "业务日期": "2026-11-15", "时区": "UTC",
                               "断点序号": 7, "断点说明": "保留这个断点",
                               "入队时间": "2026-11-15T00:00:00+00:00"})
        rerun = self.svc.list_rerun_candidates()[0]
        log, msg = self.svc.recover(rerun["id"])
        self.assertIn("已从就绪快照恢复", msg)
        # 等待队列回到重跑恢复，断点保留
        wait = self.store.rows("dep_wait_event")[0]
        self.assertEqual(wait["队列状态"], dom.QUEUE_RECOVERED)
        self.assertEqual(wait["断点序号"], 7)
        self.assertEqual(wait["断点说明"], "保留这个断点")
        # 运行面状态随水位恢复
        self.assertEqual(self.store.rows("runtime")[0]["status"], "运行中")
        # 快照进入重跑恢复且带来源水位信息
        snap = self.store.rows("dep_ledger")[0]
        self.assertEqual(snap["就绪状态"], dom.STATE_RECOVERED)
        self.assertEqual(log["保留断点序号"], 7)
        # 重复恢复只返回首次留痕
        log2, _ = self.svc.recover(rerun["id"])
        self.assertEqual(log["id"], log2["id"])
        self.assertEqual(len(self.store.rows("dep_recovery_log")), 1)


class TransactionTest(unittest.TestCase):
    def test_rollback_restores_all_tables(self) -> None:
        store = FreshStore()
        before = {name: len(rows) for name, rows in store._tables.items()}
        with self.assertRaises(RuntimeError):
            with store.transaction():
                store.rows("dep_watermark").append({"id": 99, "事件编号": "X"})
                store.rows("dep_ledger").append({"id": 99, "快照编号": "Y"})
                raise RuntimeError("boom")
        after = {name: len(rows) for name, rows in store._tables.items()}
        self.assertEqual(before, after)


if __name__ == "__main__":
    unittest.main()
