from typing import Optional

from src.tanuki.data_backend.data_backend import DataBackend
from src.tanuki.data_store.index.index import Index
from src.tanuki.data_store.index.index_alias import IndexAlias
from src.tanuki.database.data_token import DataToken


class MockBackend(DataBackend):
    _link_token: DataToken
    _columns: list[str]

    def __init__(self, link_token: DataToken, columns: list[str]) -> None:
        self._link_token = link_token
        self._columns = columns

    def is_link(self: "MockBackend") -> bool:
        return True

    def link_token(self: "MockBackend") -> Optional[DataToken]:
        return self._link_token

    def __getitem__(self, item):
        return None

    def getitems(self, item: list[str]):
        return []

    def get_index(self, index_alias: IndexAlias) -> Index:
        return None

    @property
    def columns(self) -> list[str]:
        return self._columns

    @property
    def index(self):
        return []

