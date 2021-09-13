from re import compile, Pattern
import sys
from typing import Type, _eval_type, cast, ClassVar, ForwardRef

from .column import Column
from .column_alias import ColumnAlias
from .index import Index
from .index_alias import IndexAlias


class StorableTypeFactory:
    TYPE_MATCHER: ClassVar[Pattern] = compile(r"^Index(?:\[(.*)\])$")
    
    columns: dict[str, ColumnAlias]
    indices: dict[str, IndexAlias]
    
    def __init__(self, bases: list[type], type_annotations: dict[str, str]) -> None:
        self.columns = self.eval_columns(bases, type_annotations)
        self.indices = self.eval_indices(type_annotations)

    def eval_columns(self, bases: list[type], type_annotations: dict[str, str]) -> dict[str, ColumnAlias]:
        columns = {}
        missing_types = []
        for name, type_str in type_annotations.items():
            if type_str[:6] != "Column":
                continue
            col_type = self._eval_column_type(bases, type_str)
            if col_type is Column or type(col_type) is Column:
                missing_types.append(name)
            elif col_type is ColumnAlias:
                columns[name] = col_type
            elif type(col_type) is ColumnAlias:
                cast(ColumnAlias, col_type).name = name
                columns[name] = col_type
        if len(missing_types) > 0:
            raise TypeError(
                f"Failed to find column types for the following columns: {missing_types}"
            )
        return columns

    @staticmethod
    def _eval_column_type(bases: list[type], type_str: str) -> Type:
        ref = ForwardRef(type_str, is_argument=False)
        found_type = None
        for base in reversed(bases):
            base_globals = sys.modules[base.__module__].__dict__
            try:
                found_type = _eval_type(ref, base_globals, None)
            except:
                pass
        if found_type is None:
            raise TypeError(f"Failed to derive type {found_type}")
        return found_type


    def eval_indices(self, type_annotations: dict[str, str]) -> dict[str, IndexAlias]:
        indices = {}
        for name, type_str in type_annotations.items():
            match = self.TYPE_MATCHER.match(type_str)
            if match is not None:
                type_args: str = match.group(1)
                type_args = [type_arg.strip() for type_arg in type_args.split(",")]
                indices[name] = self._eval_index(name, type_args)
        return indices

    def _eval_index(self, index_name: str, type_args: list[str]) -> IndexAlias:
        if len(type_args) == 0:
            raise TypeError(f"No columns were attached to index {index_name}")
        
        missing_columns = set(type_args) - set(self.columns.keys())
        if len(missing_columns) > 0:
            raise TypeError(f"Failed to find columns: {missing_columns}")

        column_aliases = [self.columns[column] for column in type_args]
        return IndexAlias(index_name, column_aliases)
