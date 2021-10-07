from __future__ import annotations

from dataclasses import asdict
from datetime import datetime
from typing import Any, Type, TypeVar

from dacite import Config, from_dict
from dateutil.parser import parse
from dataclasses import fields

M = TypeVar("M", bound="Metadata")


class Metadata:
    @classmethod
    def from_dict(cls: Type[M], data_dict: dict[str, Any]) -> M:
        return from_dict(data_class=cls, data=data_dict, config=Config({datetime: parse}))

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        for field in fields(self):
            if field.type is datetime:
                result[field.name] = str(result[field.name])
        return result

