from __future__ import annotations

from tanuki.database.data_token import DataToken


class DatabaseIOError(Exception):
    
    def __str__(self) -> str:
        str_args = [str(arg) for arg in self.args]
        return "\n".join(str_args)

    def __repr__(self) -> str:
        return str(self)


class DatabaseAdapterUsageError(DatabaseIOError):
    def __init__(self: DatabaseAdapterUsageError, method: str) -> None:
        super(DatabaseAdapterUsageError, self).__init__(
            f"Data adapter wasn't opened before method call: '{method}'\nUse 'with adapter:'"
        )


class DatabaseAdapterError(DatabaseIOError):
    def __init__(
        self: DatabaseAdapterError, reason: str, *exception: Exception
    ) -> None:
        super(DatabaseAdapterError, self).__init__(
            f"Database command failed, rolling back: {reason}", *exception
        )


class DatabaseCorruptionError(DatabaseIOError):
    def __init__(
        self: DatabaseCorruptionError, reason: str, *exception: Exception
    ) -> None:
        super(DatabaseCorruptionError, self).__init__(
            f"Database in corrupted state: {reason}", *exception
        )


class MissingGroupError(DatabaseIOError):
    def __init__(self: MissingTableError, data_group: str) -> None:
        super(MissingTableError, self).__init__(
            f"Failed to find data group for {data_group}"
        )


class MissingTableError(DatabaseIOError):
    def __init__(self: MissingTableError, data_token: DataToken) -> None:
        super(MissingTableError, self).__init__(
            f"Failed to find table for {data_token}"
        )


class MissingDataStoreTypeError(DatabaseIOError):
    def __init__(
        self: MissingDataStoreTypeError, store_type: str, store_version: int
    ) -> None:
        super(MissingDataStoreTypeError, self).__init__(
            f"Failed to find DataStore type for {store_type}, version {store_version}"
        )
