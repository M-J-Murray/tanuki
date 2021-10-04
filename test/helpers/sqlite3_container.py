from pathlib import Path
import shutil
from helpers.database_container import DatabaseContainer

from tanuki.database.connection_config import ConnectionConfig


class Sqlite3Container(DatabaseContainer):
    _db_dir: Path

    def __init__(self, db_dir: str) -> None:
        self._db_dir = Path(db_dir)

    def connection_config(self) -> ConnectionConfig:
        return ConnectionConfig.from_uri(self._db_dir)

    def start(self):
        self._db_dir.mkdir(parents=True, exist_ok=True)

    def reset(self):
        self.stop()
        self.start()

    def stop(self):
        if self._db_dir.exists():
            shutil.rmtree(self._db_dir)
