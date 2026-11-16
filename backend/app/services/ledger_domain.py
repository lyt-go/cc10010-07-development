"""依赖就绪账本领域模型：时间口径、水位版本裁决与就绪状态机。

账本要同时承接三类会改变等待水位的输入：
- 跨日偏移（依赖关系上的「偏移量分钟」可能把下游批次日推到前一天/后一天）；
- 时区变化（同一上游在不同时区各报一版水位，落到 UTC 才能比较）；
- 同一上游多个运行结果（迟到的完成事件要更正已过期但尚未触发的依赖）。

这里只放纯函数与常量，不直接读写 store，方便单测覆盖各种边界。
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any

# 依赖就绪账本三张表的名字（与 seed / router 保持一致）
WATERMARK_TABLE = "dep_watermark"   # 上游运行水位
WAIT_TABLE = "dep_wait_event"       # 等待事件（等待队列断点）
LEDGER_TABLE = "dep_ledger"         # 依赖就绪数据快照（账本）
RECOVERY_TABLE = "dep_recovery_log"  # 重跑恢复留痕（另一个业务面：运行面状态恢复）

# 就绪快照状态：等待中 / 已就绪可下发 / 等待超时 / 已按重跑恢复回等待
STATE_WAITING = "等待中"
STATE_READY = "已就绪"
STATE_TIMEOUT = "等待超时"
STATE_RECOVERED = "重跑恢复"

LEDGER_STATES = (STATE_WAITING, STATE_READY, STATE_TIMEOUT, STATE_RECOVERED)

# 等待事件队列状态
QUEUE_WAITING = "等待中"
QUEUE_READY = "已触发"
QUEUE_TIMEOUT = "已超时"
QUEUE_RECOVERED = "重跑恢复"

DEFAULT_TIMEOUT_MINUTES = 240   # 依赖关系没写等待超时时的兜底
DEFAULT_OFFSET_MINUTES = 0
DEFAULT_TRIGGER = "上游成功"

# 时区标签 -> 固定偏移。业务库只认这些标签，避免依赖系统时区数据库。
TIMEZONE_LABELS: dict[str, timezone] = {
    "UTC": timezone.utc,
    "Asia/Shanghai": timezone(timedelta(hours=8)),
    "UTC+8": timezone(timedelta(hours=8)),
    "UTC+9": timezone(timedelta(hours=9)),
    "Asia/Tokyo": timezone(timedelta(hours=9)),
    "Europe/Berlin": timezone(timedelta(hours=1)),
    "UTC+1": timezone(timedelta(hours=1)),
    "America/Los_Angeles": timezone(timedelta(hours=-8)),
    "UTC-8": timezone(timedelta(hours=-8)),
}


def parse_tz(label: str | None) -> timezone:
    """把时区标签解析成固定偏移；未知标签按 UTC 兜底，保证比较口径一致。"""
    if not label:
        return timezone.utc
    label = str(label).strip()
    if label in TIMEZONE_LABELS:
        return TIMEZONE_LABELS[label]
    if label.upper().startswith("UTC") and len(label) > 3:
        sign = 1
        body = label[3:]
        if body.startswith(("+", "-")):
            sign = -1 if body[0] == "-" else 1
            body = body[1:]
        if ":" in body:
            hour_part, minute_part = body.split(":", 1)
        else:
            hour_part, minute_part = body, "0"
        try:
            return timezone(sign * timedelta(hours=int(hour_part), minutes=int(minute_part)))
        except ValueError:
            return timezone.utc
    return timezone.utc


def parse_int(value: Any, default: int = 0) -> int:
    """偏移量、超时、断点序号这些字段在示例数据里可能是字符串，统一转 int。"""
    if value is None or value == "":
        return default
    try:
        return int(float(str(value).strip()))
    except (TypeError, ValueError):
        return default


def parse_biz_date(value: Any) -> date:
    """解析业务批次日（YYYY-MM-DD）；解析不了按当天兜底。"""
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value or "").strip()[:10]
    try:
        return datetime.strptime(text, "%Y-%m-%d").date()
    except ValueError:
        return datetime.now(timezone.utc).date()


def parse_dt(value: Any, tz_label: str | None = None) -> datetime:
    """解析时间戳并带上时区。

    - 已经带时区信息的时间戳直接转 UTC；
    - 不带时区的时间戳按传入的时区标签本地化（水位上报时的本地时区）。
    所有比较都在 UTC 下进行，这样跨时区的两版水位才有统一裁决口径。
    """
    tz = parse_tz(tz_label)
    if isinstance(value, datetime):
        dt = value
    else:
        text = str(value or "").strip().replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(text)
        except ValueError:
            dt = datetime.now(tz)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=tz)
    return dt.astimezone(timezone.utc)


def shift_biz_date(biz_date: date, offset_minutes: int) -> date:
    """跨日偏移：按偏移分钟把下游业务批次日平移到真正要等的上游批次日。"""
    return biz_date + timedelta(minutes=offset_minutes)


def local_midnight(biz_date: date, tz_label: str | None) -> datetime:
    """某个业务批次日在指定时区的零点，返回 UTC 时间，用于和水位时间戳比较。"""
    tz = parse_tz(tz_label)
    return datetime(biz_date.year, biz_date.month, biz_date.day, tzinfo=tz).astimezone(timezone.utc)


def iso(dt: datetime) -> str:
    """统一输出带偏移的 ISO 字符串，前端刷新后看到的口径完全一致。"""
    return dt.astimezone(timezone.utc).isoformat()


@dataclass(frozen=True)
class Watermark:
    """裁决后的上游水位视图（已统一到 UTC）。"""

    job: str
    biz_date: date
    event_time: datetime
    run_id: str
    source_tz: str
    seq: int

    @property
    def key(self) -> tuple[str, date]:
        return self.job, self.biz_date


def choose_watermark(candidates: list[dict[str, Any]]) -> dict[str, Any] | None:
    """同一上游同一批次日有多版水位时裁决采用哪一版。

    裁决顺序（确定性，不依赖入库先后的偶然性）：
    1. 事件时间更晚的优先——迟到的完成事件更正较早的运行结果；
    2. 事件时间相同则比较批次/事件序号 seq，序号大的优先；
    3. 仍然相同再按上报时间、记录 id 兜底，保证结果稳定可复算。
    不同时区先各自转 UTC 再比较，避免本地时间字符串直接排序造成的错判。
    """
    if not candidates:
        return None

    def sort_key(row: dict[str, Any]) -> tuple:
        event_time = parse_dt(row.get("事件时间"), row.get("时区"))
        return (
            event_time,
            parse_int(row.get("事件序号"), 0),
            parse_dt(row.get("上报时间"), row.get("时区")),
            int(row.get("id", 0)),
        )

    return max(candidates, key=sort_key)


def watermark_view(row: dict[str, Any]) -> Watermark:
    return Watermark(
        job=str(row.get("上游作业", "")),
        biz_date=parse_biz_date(row.get("业务日期")),
        event_time=parse_dt(row.get("事件时间"), row.get("时区")),
        run_id=str(row.get("运行编号", "")),
        source_tz=str(row.get("时区", "UTC")),
        seq=parse_int(row.get("事件序号"), 0),
    )


def relation_offset(relation: dict[str, Any]) -> int:
    return parse_int(relation.get("偏移量分钟"), DEFAULT_OFFSET_MINUTES)


def relation_timeout(relation: dict[str, Any]) -> int:
    timeout = parse_int(relation.get("等待超时"), 0)
    return timeout if timeout > 0 else DEFAULT_TIMEOUT_MINUTES
