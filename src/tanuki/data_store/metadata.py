from __future__ import annotations

from datetime import datetime
from typing import Any, ClassVar, Optional, Type, TypeVar

from dateutil.parser import parse
from pydantic.generics import GenericModel

M = TypeVar("M", bound="Metadata")


class Metadata(GenericModel):
    # Class vars
    _registered_metadata: ClassVar[dict[str, dict[int, Type[M]]]] = {}
    version: ClassVar[int]

    @classmethod
    def metadata_type(
        cls, metadata_class: str, metadata_version: int
    ) -> Optional[Type[Metadata]]:
        return cls._registered_metadata.get(metadata_class, {}).get(metadata_version)

    def __init_subclass__(
        cls: Type[M], version: int = 1, register: bool = True
    ) -> None:
        super(Metadata, cls).__init_subclass__()
        if register and Metadata.metadata_type(cls.__name__, version) is not None:
            raise TypeError(
                f"Duplicate Metadata version found for {cls} version {version}"
            )
        cls.version = version
        if register:
            if cls.__name__ not in Metadata._registered_metadata:
                Metadata._registered_metadata[cls.__name__] = {}
            Metadata._registered_metadata[cls.__name__][version] = cls

    @classmethod
    def from_dict(cls: Type[M], data_dict: dict[str, Any]) -> M:
        data_copy = {}
        for field in cls.__fields__.values():
            field_data = data_dict[field.name]
            if field.type_ is datetime:
                data_copy[field.name] = parse(field_data)
            else:
                data_copy[field.name] = field_data
        return cls(**data_copy)

    def to_dict(self) -> dict[str, Any]:
        result = self.dict()
        for field in self.__fields__.values():
            if field.type_ is datetime:
                result[field.name] = str(result[field.name])
        return result
