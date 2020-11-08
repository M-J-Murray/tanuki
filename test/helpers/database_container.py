from src.database.connection_config import ConnectionConfig


class DatabaseContainer:
    def connection_config(self) -> ConnectionConfig:
        ...

    def start(self):
        ...

    def reset(self):
        ...

    def stop(self):
        ...
