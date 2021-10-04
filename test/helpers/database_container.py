from src.tanuki.database.connection_config import ConnectionConfig


class DatabaseContainer:
    def connection_config(self) -> ConnectionConfig:
        raise NotImplementedError()

    def start(self):
        raise NotImplementedError()

    def reset(self):
        raise NotImplementedError()

    def stop(self):
        raise NotImplementedError()
