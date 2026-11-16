"""依赖就绪账本服务：水位/等待事件入库、就绪快照重算与重跑恢复。

设计要点对应需求：
- 可持久化：账本、水位、等待事件、恢复留痕都是 store 里的独立表，下游可随时读取；
- 同事务：水位与就绪快照在 self.store.transaction() 内一起提交，失败整体回滚；
- 幂等：相同事件键（事件编号）连续提交只生效一次；就绪快照按业务键做版本演进，
  不重复生成下游可执行结果；
- 跨日/时区/多运行结果：统一转 UTC、按偏移平移批次日、按确定性规则裁决水位版本；
- 迟到更正：较晚到达的完成事件会把「已过期但尚未触发（仍在等待）」的依赖改写为就绪；
- 超时落重跑清单：等待超时的业务结果生成/更新异常重跑记录，随依赖解析一起重算；
- 重跑恢复：从就绪快照恢复等待队列与运行面状态，断点保留在等待事件上，可核对。
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from app.services.ledger_domain import (
    DEFAULT_TRIGGER,
    LEDGER_TABLE,
    QUEUE_READY,
    QUEUE_RECOVERED,
    QUEUE_TIMEOUT,
    QUEUE_WAITING,
    RECOVERY_TABLE,
    STATE_READY,
    STATE_RECOVERED,
    STATE_TIMEOUT,
    STATE_WAITING,
    WAIT_TABLE,
    WATERMARK_TABLE,
    Watermark,
    choose_watermark,
    iso,
    parse_biz_date,
    parse_dt,
    parse_int,
    relation_offset,
    relation_timeout,
    shift_biz_date,
    watermark_view,
)
from app.store import store

RERUN_MODULE = "rerun"
DEPENDENCY_MODULE = "dependency"
RUNTIME_MODULE = "runtime"


def _now() -> datetime:
    return datetime.now(timezone.utc)


class DependencyLedgerService:
    def __init__(self, data_store: Any = None) -> None:
        # 默认用进程级内存库；测试可注入独立 Store，避免污染示例数据
        self.store = data_store or store

    # ------------------------------------------------------------------ 查询
    def list_ledger(
        self,
        *,
        keyword: str | None = None,
        state: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = self.store.rows(LEDGER_TABLE)
        if keyword:
            rows = [r for r in rows if keyword in str(r.get("快照编号", "")) or keyword in str(r.get("下游作业", ""))]
        if state:
            rows = [r for r in rows if r.get("就绪状态") == state]
        total = len(rows)
        start = max(page - 1, 0) * size
        return rows[start:start + size], total

    def list_watermarks(self, *, job: str | None = None) -> list[dict[str, Any]]:
        rows = self.store.rows(WATERMARK_TABLE)
        if job:
            rows = [r for r in rows if r.get("上游作业") == job]
        return rows

    def list_waiting_queue(self, *, include_finished: bool = True) -> list[dict[str, Any]]:
        rows = self.store.rows(WAIT_TABLE)
        if not include_finished:
            rows = [r for r in rows if r.get("队列状态") == QUEUE_WAITING]
        return rows

    def list_recovery_logs(self) -> list[dict[str, Any]]:
        return self.store.rows(RECOVERY_TABLE)

    def get_ledger(self, entry_id: int) -> dict[str, Any] | None:
        return self.store.find(LEDGER_TABLE, entry_id)

    def stats(self) -> list[dict[str, Any]]:
        rows = self.store.rows(LEDGER_TABLE)
        return [
            {"label": "就绪快照总数", "value": len(rows)},
            {"label": "等待中", "value": sum(1 for r in rows if r.get("就绪状态") == STATE_WAITING)},
            {"label": "已就绪可执行", "value": sum(1 for r in rows if r.get("就绪状态") == STATE_READY)},
            {"label": "等待超时", "value": sum(1 for r in rows if r.get("就绪状态") == STATE_TIMEOUT)},
            {"label": "重跑恢复", "value": sum(1 for r in rows if r.get("就绪状态") == STATE_RECOVERED)},
        ]

    # ------------------------------------------------------------ 事件入库
    def ingest_watermark(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, str, bool]:
        """登记一条上游运行水位。

        返回 (记录, 说明, 是否新建)。相同事件编号连续提交只生效一次（幂等）。
        水位与由它触发的就绪快照在同一事务里落库。
        """
        event_code = str(values.get("事件编号") or "").strip()
        job = str(values.get("上游作业") or "").strip()
        if not event_code or not job:
            return None, "缺少必填字段：事件编号、上游作业", False
        biz_date = parse_biz_date(values.get("业务日期"))
        tz_label = str(values.get("时区") or "UTC")

        with self.store.transaction():
            existing = self._find_by_code(WATERMARK_TABLE, "事件编号", event_code)
            if existing is not None:
                # 同一就绪/水位事件连续提交：直接返回已生效记录，不重复落库
                return existing, f"水位事件 {event_code} 已存在，重复提交不重复生效", False

            row = {
                "id": self.store.next_id(WATERMARK_TABLE),
                "事件编号": event_code,
                "上游作业": job,
                "业务日期": biz_date.isoformat(),
                "运行编号": str(values.get("运行编号") or event_code),
                "事件时间": iso(parse_dt(values.get("事件时间"), tz_label)),
                "事件序号": parse_int(values.get("事件序号"), 1),
                "时区": tz_label,
                "触发条件": str(values.get("触发条件") or DEFAULT_TRIGGER),
                "上报时间": iso(_now()),
            }
            self.store.rows(WATERMARK_TABLE).append(row)
            # 新水位可能立刻满足某个等待中的依赖：同事务重算，保证水位与快照一起可见
            self._recompute_locked()
        return row, f"水位事件 {event_code} 已登记并完成依赖重算", True

    def enqueue_wait(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, str, bool]:
        """把一条依赖等待放入等待队列（等待事件），随后同事务重算。"""
        event_code = str(values.get("等待编号") or "").strip()
        relation_code = str(values.get("关系编号") or "").strip()
        if not event_code or not relation_code:
            return None, "缺少必填字段：等待编号、关系编号", False
        relation = self._find_relation(relation_code)
        if relation is None:
            return None, f"依赖关系 {relation_code} 不存在或未生效", False

        biz_date = parse_biz_date(values.get("业务日期"))
        tz_label = str(values.get("时区") or "UTC")
        enqueued = parse_dt(values.get("入队时间"), tz_label) if values.get("入队时间") else _now()
        with self.store.transaction():
            existing = self._find_by_code(WAIT_TABLE, "等待编号", event_code)
            if existing is not None:
                return existing, f"等待事件 {event_code} 已存在，重复提交不重复生效", False
            row = {
                "id": self.store.next_id(WAIT_TABLE),
                "等待编号": event_code,
                "关系编号": relation_code,
                "上游作业": relation.get("上游作业"),
                "下游作业": relation.get("下游作业"),
                "业务日期": biz_date.isoformat(),
                "时区": tz_label,
                "断点序号": parse_int(values.get("断点序号"), 1),
                "断点说明": str(values.get("断点说明") or "依赖解析挂起，等待上游水位"),
                "队列状态": QUEUE_WAITING,
                "入队时间": iso(enqueued),
                "更新时间": iso(enqueued),
            }
            self.store.rows(WAIT_TABLE).append(row)
            self._recompute_locked()
        return row, f"等待事件 {event_code} 已入队并完成依赖重算", True

    def recompute(self) -> dict[str, Any]:
        """对外的手动重算入口：重新裁决全部等待事件并演进就绪快照。"""
        with self.store.transaction():
            summary = self._recompute_locked()
        return summary

    # ------------------------------------------------------------ 核心重算
    def _recompute_locked(self) -> dict[str, Any]:
        """在已开启的事务内执行重算；调用方负责事务边界。"""
        ready_n = waiting_n = timeout_n = rerun_n = corrected_n = 0
        active_relations = {
            str(r.get("关系编号")): r for r in self.store.rows(DEPENDENCY_MODULE) if r.get("status") == "已生效"
        }
        for wait in list(self.store.rows(WAIT_TABLE)):
            relation = active_relations.get(str(wait.get("关系编号")))
            if relation is None:
                continue
            outcome = self._evaluate(relation, wait)
            snapshot = self._upsert_snapshot(relation, wait, outcome)
            # 等待事件队列状态与快照保持一致（同一个业务事实的两个视图）
            wait["队列状态"] = outcome["队列状态"]
            wait["更新时间"] = iso(_now())
            state = snapshot["就绪状态"]
            if state == STATE_READY:
                ready_n += 1
            elif state == STATE_TIMEOUT:
                timeout_n += 1
                if self._ensure_rerun(snapshot, relation):
                    rerun_n += 1
            else:
                waiting_n += 1
            if snapshot.get("版本号", 1) > 1 and state in (STATE_READY, STATE_TIMEOUT):
                corrected_n += 1
        return {
            "已就绪": ready_n,
            "等待中": waiting_n,
            "等待超时": timeout_n,
            "新增重跑": rerun_n,
            "迟到更正": corrected_n,
            "重算时间": iso(_now()),
        }

    def _evaluate(self, relation: dict[str, Any], wait: dict[str, Any]) -> dict[str, Any]:
        """针对单条等待事件裁决当前就绪结论（纯读，不写表）。"""
        offset = relation_offset(relation)
        timeout_minutes = relation_timeout(relation)
        trigger = str(relation.get("触发条件") or DEFAULT_TRIGGER)
        biz_date = parse_biz_date(wait.get("业务日期"))
        # 跨日偏移：下游批次日 + 偏移量 = 真正要等的上游批次日
        upstream_biz_date = shift_biz_date(biz_date, offset)
        tz_label = str(wait.get("时区") or "UTC")
        needed_at = parse_dt(wait.get("入队时间"), tz_label)
        deadline = needed_at.replace(microsecond=0) + timedelta(minutes=timeout_minutes)
        now = _now()

        candidates = [
            wm for wm in self.store.rows(WATERMARK_TABLE)
            if str(wm.get("上游作业")) == str(relation.get("上游作业"))
            and parse_biz_date(wm.get("业务日期")) == upstream_biz_date
        ]
        chosen_raw = choose_watermark(candidates)
        chosen: Watermark | None = watermark_view(chosen_raw) if chosen_raw else None

        result = {
            "上游批次日": upstream_biz_date.isoformat(),
            "偏移量分钟": offset,
            "等待超时分钟": timeout_minutes,
            "触发条件": trigger,
            "等待截止": iso(deadline),
            "采用水位编号": chosen_raw.get("事件编号") if chosen_raw else None,
            "采用运行编号": chosen.run_id if chosen else None,
            "来源时区": chosen.source_tz if chosen else tz_label,
            "来源事件时间": iso(chosen.event_time) if chosen else None,
            "裁决版本数": len(candidates),
            "队列状态": QUEUE_WAITING,
        }

        if chosen is not None and self._meets_trigger(chosen_raw, trigger):
            # 上游水位满足触发条件：生成下游可执行结果（只在此时产出 ready）
            result["就绪状态"] = STATE_READY
            result["队列状态"] = QUEUE_READY
            result["超时原因"] = ""
        elif now > deadline:
            # 超过等待截止仍未满足：超时，业务结果随后落入异常重跑清单
            result["就绪状态"] = STATE_TIMEOUT
            result["队列状态"] = QUEUE_TIMEOUT
            result["超时原因"] = (
                f"等待上游 {relation.get('上游作业')} 批次 {upstream_biz_date.isoformat()} "
                f"超过 {timeout_minutes} 分钟仍未满足「{trigger}」"
            )
        else:
            result["就绪状态"] = STATE_WAITING
            result["超时原因"] = ""
        return result

    @staticmethod
    def _meets_trigger(watermark_row: dict[str, Any], trigger: str) -> bool:
        """触发条件判断：默认要求上游成功；跳过类条件不卡执行状态。"""
        if "跳过" in trigger or "无条件" in trigger:
            return True
        condition = str(watermark_row.get("触发条件") or DEFAULT_TRIGGER)
        # 水位事件本身标记为成功，或触发条件明确写了成功/完成，都视为满足
        return "成功" in condition or "完成" in condition or condition == DEFAULT_TRIGGER

    def _upsert_snapshot(
        self, relation: dict[str, Any], wait: dict[str, Any], outcome: dict[str, Any]
    ) -> dict[str, Any]:
        """按业务键演进就绪快照：同一等待事件只有一条快照，靠版本号留痕。

        连续提交同一事件只生效一次：状态没变就不升版本；迟到水位把「等待中」改成
        「已就绪」、或把未触发的等待改成超时，才追加新版本并保留上一版原因。
        """
        wait_code = str(wait.get("等待编号"))
        existing = next(
            (r for r in self.store.rows(LEDGER_TABLE) if r.get("等待编号") == wait_code), None
        )
        common = {
            "关系编号": relation.get("关系编号"),
            "上游作业": relation.get("上游作业"),
            "下游作业": relation.get("下游作业"),
            "等待编号": wait_code,
            "业务日期": wait.get("业务日期"),
            "上游批次日": outcome["上游批次日"],
            "偏移量分钟": outcome["偏移量分钟"],
            "触发条件": outcome["触发条件"],
            "等待超时分钟": outcome["等待超时分钟"],
            "等待截止": outcome["等待截止"],
            "采用水位编号": outcome["采用水位编号"],
            "采用运行编号": outcome["采用运行编号"],
            "来源时区": outcome["来源时区"],
            "来源事件时间": outcome["来源事件时间"],
            "裁决版本数": outcome["裁决版本数"],
            "超时原因": outcome["超时原因"],
            "断点序号": wait.get("断点序号"),
            "断点说明": wait.get("断点说明"),
        }
        if existing is None:
            new_id = self.store.next_id(LEDGER_TABLE)
            row = {
                "id": new_id,
                "快照编号": f"DRL-{new_id:04d}",
                "版本号": 1,
                "就绪状态": outcome["就绪状态"],
                "上一状态": "",
                **common,
                "创建时间": iso(_now()),
                "更新时间": iso(_now()),
            }
            self._apply_flags(row)
            self.store.rows(LEDGER_TABLE).append(row)
            return row

        # 已存在快照：状态未变即幂等返回，不新增版本、不重复下发下游
        if existing.get("就绪状态") == outcome["就绪状态"]:
            existing.update({k: v for k, v in common.items() if v is not None})
            existing["更新时间"] = iso(_now())
            self._apply_flags(existing)
            return existing

        existing["上一状态"] = existing.get("就绪状态")
        existing["就绪状态"] = outcome["就绪状态"]
        existing["版本号"] = parse_int(existing.get("版本号"), 1) + 1
        existing.update(common)
        existing["更新时间"] = iso(_now())
        self._apply_flags(existing)
        return existing

    @staticmethod
    def _apply_flags(row: dict[str, Any]) -> None:
        row["pending"] = row.get("就绪状态") in (STATE_WAITING, STATE_TIMEOUT, STATE_RECOVERED)
        row["abnormal"] = row.get("就绪状态") in (STATE_TIMEOUT, STATE_RECOVERED)

    def _ensure_rerun(self, snapshot: dict[str, Any], relation: dict[str, Any]) -> bool:
        """超时业务结果落入异常重跑清单；同一快照的重跑单只维护一条（幂等）。"""
        snapshot_code = str(snapshot.get("快照编号"))
        reruns = self.store.rows(RERUN_MODULE)
        existing = next((r for r in reruns if r.get("来源快照编号") == snapshot_code), None)
        if existing is not None:
            # 重算时刷新原因与状态，不重复建单
            existing["重跑原因"] = snapshot.get("超时原因")
            existing["重跑状态"] = "等待重算"
            existing["abnormal"] = True
            return False
        new_id = self.store.next_id(RERUN_MODULE)
        entry = {
            "id": new_id,
            "status": "草稿",
            "pending": True,
            "abnormal": True,
            "任务编号": f"RERU-{new_id:04d}",
            "原始运行编号": snapshot.get("采用运行编号") or "待上游补跑",
            "重跑区间开始": snapshot.get("上游批次日"),
            "重跑区间结束": snapshot.get("业务日期"),
            "重跑原因": snapshot.get("超时原因"),
            "覆盖策略": "按依赖关系级联重算",
            "依赖级联": f"{relation.get('上游作业')} -> {relation.get('下游作业')}",
            "重跑状态": "等待重算",
            "来源快照编号": snapshot_code,
            "来源等待编号": snapshot.get("等待编号"),
            "上游作业": relation.get("上游作业"),
            "下游作业": relation.get("下游作业"),
        }
        reruns.append(entry)
        return True

    # ------------------------------------------------------------ 重跑恢复
    def recover(self, rerun_id: int) -> tuple[dict[str, Any] | None, str]:
        """依据依赖关系从就绪快照恢复等待队列，并恢复运行面状态。

        - 等待队列：把对应等待事件从超时/已触发恢复为等待中，快照回到「重跑恢复」；
        - 运行面：按上游作业找到受影响运行记录，保存原状态后回退为运行中，留痕可核对；
        - 断点：等待事件上的断点序号/说明原样保留；
        - 幂等：同一重跑单重复恢复只返回上次留痕，不重复改写。
        """
        rerun = self.store.find(RERUN_MODULE, rerun_id)
        if rerun is None:
            return None, f"重跑任务 {rerun_id} 不存在或已归档"
        with self.store.transaction():
            log = next(
                (r for r in self.store.rows(RECOVERY_TABLE) if r.get("重跑任务编号") == rerun.get("任务编号")),
                None,
            )
            if log is not None:
                return log, f"重跑任务 {rerun.get('任务编号')} 已恢复，重复操作不重复生效"

            wait_code = rerun.get("来源等待编号")
            wait = next((w for w in self.store.rows(WAIT_TABLE) if w.get("等待编号") == wait_code), None)
            snapshot = next(
                (s for s in self.store.rows(LEDGER_TABLE) if s.get("快照编号") == rerun.get("来源快照编号")),
                None,
            )
            recovered_waits: list[str] = []
            restored_runs: list[dict[str, Any]] = []
            if wait is not None:
                wait["队列状态"] = QUEUE_RECOVERED
                wait["更新时间"] = iso(_now())
                recovered_waits.append(str(wait.get("等待编号")))
            if snapshot is not None:
                snapshot["上一状态"] = snapshot.get("就绪状态")
                snapshot["就绪状态"] = STATE_RECOVERED
                snapshot["更新时间"] = iso(_now())
                self._apply_flags(snapshot)

            # 另一个业务面：运行面状态随水位恢复
            upstream_job = rerun.get("上游作业")
            for run in self.store.rows(RUNTIME_MODULE):
                if run.get("作业名称") == upstream_job:
                    restored_runs.append({
                        "运行编号": run.get("记录编号"),
                        "原状态": run.get("status"),
                        "恢复后状态": "运行中",
                    })
                    run["status"] = "运行中"
                    run["pending"] = True
                    run["abnormal"] = True

            rerun["status"] = "执行中"
            rerun["重跑状态"] = "已恢复等待"
            rerun["pending"] = True
            new_id = self.store.next_id(RECOVERY_TABLE)
            log = {
                "id": new_id,
                "恢复编号": f"DRCV-{new_id:04d}",
                "重跑任务编号": rerun.get("任务编号"),
                "来源快照编号": rerun.get("来源快照编号"),
                "来源等待编号": wait_code,
                "恢复等待事件": recovered_waits,
                "恢复运行记录": restored_runs,
                "保留断点序号": wait.get("断点序号") if wait else None,
                "保留断点说明": wait.get("断点说明") if wait else None,
                "恢复时间": iso(_now()),
            }
            self.store.rows(RECOVERY_TABLE).append(log)
        return log, f"重跑任务 {rerun.get('任务编号')} 已从就绪快照恢复等待队列与运行面状态"

    def list_rerun_candidates(self) -> list[dict[str, Any]]:
        """异常重跑清单里由依赖就绪账本产生、可做恢复的重跑单。"""
        return [r for r in self.store.rows(RERUN_MODULE) if r.get("来源快照编号")]

    # ---------------------------------------------------------------- 工具
    def _find_by_code(self, table: str, field: str, code: str) -> dict[str, Any] | None:
        return next((r for r in self.store.rows(table) if r.get(field) == code), None)

    def _find_relation(self, relation_code: str) -> dict[str, Any] | None:
        for row in self.store.rows(DEPENDENCY_MODULE):
            if row.get("关系编号") == relation_code and row.get("status") == "已生效":
                return row
        return None


service = DependencyLedgerService()
