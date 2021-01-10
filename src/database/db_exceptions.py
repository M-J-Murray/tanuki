from __future__ import annotations

from src.database.data_token import DataToken


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
