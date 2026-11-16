"""依赖就绪账本接口。

对外提供四组能力：
- 就绪快照（账本）查询：下游读取「是否可执行」的唯一数据来源；
- 水位/等待事件入库：连续提交同一事件只生效一次，且与快照同事务落库；
- 手动重算与等待队列、恢复留痕查询；
- 异常重跑恢复：从快照恢复等待队列与运行面状态，断点保留可核对。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.schemas import ActionResult, EntryPayload, PageResult
from app.services.depledger import service
from app.services.ledger_domain import LEDGER_STATES

router = APIRouter(prefix="/api/depledger", tags=["依赖就绪账本"])


@router.get("/ledger", response_model=PageResult[dict])
def list_ledger(
    keyword: str | None = Query(default=None, description="按快照编号或下游作业检索"),
    state: str | None = Query(default=None, description="等待中、已就绪、等待超时、重跑恢复"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """分页读取依赖就绪数据快照；刷新页面读到的就是已落库的同一版口径。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    if state and state not in LEDGER_STATES:
        raise HTTPException(status_code=400, detail=f"就绪状态仅支持：{'、'.join(LEDGER_STATES)}")
    items, total = service.list_ledger(keyword=keyword, state=state, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/ledger/stats")
def ledger_stats() -> dict[str, Any]:
    """看板统计：各就绪状态的快照数量。"""
    return {"items": service.stats()}


@router.get("/watermarks")
def list_watermarks(job: str | None = None) -> dict[str, Any]:
    """上游运行水位列表，可按上游作业过滤。"""
    items = service.list_watermarks(job=job)
    return {"items": items, "total": len(items)}


@router.get("/waiting")
def list_waiting(include_finished: bool = True) -> dict[str, Any]:
    """等待队列：默认含已触发/已超时/已恢复的全部断点记录。"""
    items = service.list_waiting_queue(include_finished=include_finished)
    return {"items": items, "total": len(items)}


@router.get("/recovery")
def list_recovery() -> dict[str, Any]:
    """重跑恢复留痕：记录每次恢复前后的等待队列与运行面状态。"""
    items = service.list_recovery_logs()
    return {"items": items, "total": len(items)}


@router.get("/reruns")
def list_rerun_candidates() -> dict[str, Any]:
    """异常重跑清单中由账本产生、可执行恢复的重跑单。"""
    items = service.list_rerun_candidates()
    return {"items": items, "total": len(items)}


@router.post("/watermarks", response_model=ActionResult)
def ingest_watermark(payload: EntryPayload) -> ActionResult:
    """登记上游运行水位并重算依赖；同一事件编号重复提交只生效一次。"""
    entry, message, created = service.ingest_watermark(payload.values)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)


@router.post("/waiting", response_model=ActionResult)
def enqueue_wait(payload: EntryPayload) -> ActionResult:
    """登记等待事件并入队；同一等待编号重复提交只生效一次。"""
    entry, message, created = service.enqueue_wait(payload.values)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)


@router.post("/recompute", response_model=ActionResult)
def recompute() -> ActionResult:
    """按依赖关系偏移与等待事件全量重算就绪快照，返回本次裁决汇总。"""
    summary = service.recompute()
    return ActionResult(ok=True, message="依赖就绪账本已重算", entry=summary)


@router.post("/reruns/{rerun_id}/recover", response_model=ActionResult)
def recover_rerun(rerun_id: int) -> ActionResult:
    """从就绪快照恢复等待队列与运行面状态；重复恢复只返回首次留痕。"""
    log, message = service.recover(rerun_id)
    if log is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=log)


@router.get("/export")
def export_ledger() -> dict[str, Any]:
    """导出账本全量：快照、水位、等待队列、恢复留痕一次取齐，便于离线核对。"""
    ledger, total = service.list_ledger(page=1, size=10000)
    return {
        "module": "depledger",
        "total": total,
        "items": ledger,
        "watermarks": service.list_watermarks(),
        "waiting": service.list_waiting_queue(),
        "recovery": service.list_recovery_logs(),
    }
