from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import shutil
from sqlite3 import Connection

from tanuki.database.adapter.statement.sql_statement import SqlStatement
from tanuki.database.data_token import DataToken


class UncommittedChange:
    def rollback(self, connection: Connection):
        NotImplementedError()


@dataclass
class UncommittedGroupCreate(UncommittedChange):
    data_group: DataToken
    data_group_path: Path

    def rollback(self, connection: Connection):
        self.data_group_path.unlink()

        statement = SqlStatement().DETACH_DATABASE(self.data_group).compile()
        connection.execute(statement)


@dataclass
class UncommittedGroupDelete(UncommittedChange):
    data_group_path: Path
    data_group: str
    checkpoint_dir: Path

    def checkpoint_path(self) -> Path:
        return self.checkpoint_dir / f"{self.data_group}.db"

    def checkpoint(self):
        checkpoint_path = self.checkpoint_path()
        shutil.copyfile(str(self.data_group_path), str(checkpoint_path))

    def rollback(self, connection: Connection):
        self.data_group_path.unlink()
        checkpoint_path = self.checkpoint_path()
        shutil.copyfile(str(checkpoint_path), str(self.data_group_path))
        checkpoint_path.unlink()

        statement = SqlStatement().ATTACH_DATABASE(self.data_group).compile()
        connection.execute(statement)


@dataclass
class UncommittedMetadataCreate(UncommittedChange):
    metadata_path: Path

    def rollback(self, connection: Connection):
        self.metadata_path.unlink()


@dataclass
class UncommittedMetadataDelete(UncommittedChange):
    metadata_path: Path
    data_token: DataToken
    checkpoint_dir: Path

    def checkpoint_path(self) -> Path:
        return (
            self.checkpoint_dir
            / self.data_token.data_group
            / f"{self.data_token.table_name}.json"
        )

    def checkpoint(self):
        checkpoint_path = self.checkpoint_path()
        checkpoint_path.parent.mkdir(exist_ok=True)
        shutil.copyfile(self.metadata_path, checkpoint_path)

    def rollback(self, connection: Connection):
        self.metadata_path.unlink()
        checkpoint_path = self.checkpoint_path()
        shutil.copyfile(str(checkpoint_path), str(self.metadata_path))
        checkpoint_path.unlink()


@dataclass
class UncommittedMetadataGroupDelete(UncommittedChange):
    metadata_group_path: Path
    data_group: str
    checkpoint_dir: Path

    def checkpoint_group_path(self) -> Path:
        return self.checkpoint_dir / self.data_group

    def checkpoint(self):
        checkpoint_group_path = self.checkpoint_group_path()
        shutil.copytree(str(self.metadata_group_path), str(checkpoint_group_path))

    def rollback(self, connection: Connection):
        shutil.rmtree(str(self.metadata_group_path))
        checkpoint_group_path = self.checkpoint_group_path()
        shutil.copytree(str(checkpoint_group_path), str(self.metadata_group_path))
        shutil.rmtree(str(checkpoint_group_path))
