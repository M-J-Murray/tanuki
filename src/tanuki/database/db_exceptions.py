from __future__ import annotations

from tanuki.database.data_token import DataToken


class DatabaseAdapterUsageError(IOError):
    def __init__(self: DatabaseAdapterUsageError, method: str) -> None:
        super(DatabaseAdapterUsageError, self).__init__(
            f"Data adapter wasn't opened before method call: '{method}'\nUse 'with adapter:'"
        )


class DatabaseAdapterError(IOError):
    def __init__(
        self: DatabaseAdapterError, reason: str, *exception: Exception
    ) -> None:
        super(DatabaseAdapterError, self).__init__(
            f"Database command failed, rolling back: {reason}\n", *exception
        )


class DatabaseCorruptionError(IOError):
    def __init__(
        self: DatabaseCorruptionError, reason: str, *exception: Exception
    ) -> None:
        super(DatabaseCorruptionError, self).__init__(
            f"Database in corrupted state: {reason}", *exception
        )


class MissingGroupError(IOError):
    def __init__(self: MissingTableError, data_group: str) -> None:
        super(MissingTableError, self).__init__(
            f"Failed to find data group for {data_group}"
        )


class MissingTableError(IOError):
    def __init__(self: MissingTableError, data_token: DataToken) -> None:
        super(MissingTableError, self).__init__(
            f"Failed to find table for {data_token}"
        )


class MissingDataStoreTypeError(IOError):
    def __init__(
        self: MissingDataStoreTypeError, store_type: str, store_version: int
    ) -> None:
        super(MissingDataStoreTypeError, self).__init__(
            f"Failed to find DataStore type for {store_type}, version {store_version}"
        )
