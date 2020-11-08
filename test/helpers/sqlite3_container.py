from pathlib import Path
import sqlite3
from sqlite3 import Connection
from test.helpers.database_container import DatabaseContainer
from typing import Optional

from src.database.connection_config import ConnectionConfig


class Sqlite3Container(DatabaseContainer):
    _db_path: Path
    _conn: Optional[Connection]

    def __init__(self, db_path: str) -> None:
        self._db_path = Path(db_path)
        self._conn = None

    def connection_config(self) -> ConnectionConfig:
        return ConnectionConfig.from_uri(self._db_path)

    def start(self):
        if self._conn is None:
            self._conn = sqlite3.connect(self._db_path)

    def reset(self):
        self.stop()
        self.start()

    def stop(self):
        if self._conn is not None:
            self._conn.close()
            self._conn = None
            self._db_path.unlink()
