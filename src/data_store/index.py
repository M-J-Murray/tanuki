from __future__ import annotations

from typing import Generic, TypeVar

from .column_alias import ColumnAlias

C = TypeVar("C", bound=ColumnAlias)


class Index(Generic[C]):
    def __init__(self) -> None:
        super().__init__()
