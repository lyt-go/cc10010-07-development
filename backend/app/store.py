"""内存数据仓库：给每个业务模块准备一份可筛选、可流转的示例数据。

真实项目里这里会换成数据库访问层；当前实现只依赖标准库，保证克隆下来就能起。

依赖就绪账本要求「依赖就绪记录和运行水位必须同一事务落库」，因此这里在内存表之上
提供了 snapshot/rollback 形式的事务边界：业务服务在事务里同时改多张表，任一步失败
都会回滚到进入事务前的快照，避免只写了一半的水位或就绪记录。
"""
from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Iterator

from app.seed import SEED_ROWS


class Store:
    def __init__(self) -> None:
        self._tables: dict[str, list[dict[str, Any]]] = {
            name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
        }

    def module_names(self) -> list[str]:
        return sorted(self._tables)

    def rows(self, module: str) -> list[dict[str, Any]]:
        return self._tables.setdefault(module, [])

    def find(self, module: str, entry_id: int) -> dict[str, Any] | None:
        for row in self.rows(module):
            if int(row.get("id", 0)) == entry_id:
                return row
        return None

    def next_id(self, module: str) -> int:
        """给一张表分配下一个自增主键；历史记录再乱也不会撞号。"""
        return max((int(row.get("id", 0)) for row in self.rows(module)), default=0) + 1

    @contextmanager
    def transaction(self) -> Iterator[None]:
        """同一事务里对多张表的修改要么全部生效，要么全部回滚。

        深拷贝整库快照：内存实现没有真正的 redo/undo 日志，靠快照恢复到进入事务前的
        状态，语义上等价于数据库事务的原子提交。
        """
        snapshot = {name: [dict(row) for row in rows] for name, rows in self._tables.items()}
        try:
            yield
        except Exception:
            self._tables = snapshot
            raise

    def overview(self) -> dict[str, object]:
        modules: list[dict[str, object]] = []
        for name in self.module_names():
            rows = self.rows(name)
            modules.append({
                "name": name,
                "created": len(rows),
                "pending": sum(1 for row in rows if row.get("pending")),
                "abnormal": sum(1 for row in rows if row.get("abnormal")),
            })
        cards = [
            {"label": "业务模块", "value": len(modules)},
            {"label": "今日新增", "value": sum(int(item["created"]) for item in modules)},
            {"label": "待处理", "value": sum(int(item["pending"]) for item in modules)},
            {"label": "异常量", "value": sum(int(item["abnormal"]) for item in modules)},
        ]
        return {"cards": cards, "modules": modules}


store = Store()
