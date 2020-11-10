from src.database.data_token import DataToken
from .data_backend import DataBackend


class DatabaseBackend(DataBackend):
    def __init__(
        self: "DatabaseBackend", data_token: DataToken, read_only: bool = True
    ) -> None:
        pass
