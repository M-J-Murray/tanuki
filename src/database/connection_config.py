from __future__ import annotations


class ConnectionConfig:
    _uri: str

    @classmethod
    def from_uri(cls: type[ConnectionConfig], uri: str) -> ConnectionConfig:
        conn_config = cls()
        conn_config._uri = uri
        return conn_config

    def uri(self) -> str:
        return self._uri
