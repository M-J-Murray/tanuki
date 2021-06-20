import pickle

from src.data_store.data_type import Boolean, Bytes, Int64, String
from src.database.reference_tables import (
    PROTECTED_GROUP,
    StoreDefinition,
    StoreReference,
    TableReference,
)

TABLE_REFERENCE = TableReference(
    table_name=[
        "table_reference",
        "store_reference",
        "StoreDefinition_v1_definition",
        "TableReference_v1_definition",
        "StoreReference_v1_definition",
    ],
    data_group=[
        PROTECTED_GROUP,
        PROTECTED_GROUP,
        PROTECTED_GROUP,
        PROTECTED_GROUP,
        PROTECTED_GROUP,
    ],
    store_type=[
        "TableReference",
        "StoreReference",
        "StoreDefinition",
        "StoreDefinition",
        "StoreDefinition",
    ],
    store_version=[1, 1, 1, 1, 1],
    protected=[True, True, True, True, True],
)

STORE_REFERENCE = StoreReference(
    store_type=["StoreDefinition", "TableReference", "StoreReference"],
    store_version=[1, 1, 1],
    definition_reference=[
        "StoreDefinition_v1_definition",
        "TableReference_v1_definition",
        "StoreReference_v1_definition",
    ],
    definition_version=[1, 1, 1],
)

TABLE_REFERENCE_STORE_DEFINITION = StoreDefinition(
    column_name=[
        "index",
        "table_name",
        "data_group",
        "store_type",
        "store_version",
        "protected",
    ],
    column_type=[
        pickle.dumps(Int64),
        pickle.dumps(String),
        pickle.dumps(String),
        pickle.dumps(String),
        pickle.dumps(Int64),
        pickle.dumps(Boolean),
    ],
)

STORE_REFERENCE_STORE_DEFINITION = StoreDefinition(
    column_name=[
        "index",
        "store_type",
        "store_version",
        "definition_reference",
        "definition_version",
    ],
    column_type=[
        pickle.dumps(Int64),
        pickle.dumps(String),
        pickle.dumps(Int64),
        pickle.dumps(String),
        pickle.dumps(Int64),
    ],
)

STORE_DEFINITION_STORE_DEFINITION = StoreDefinition(
    column_name=[
        "index",
        "column_name",
        "column_type",
    ],
    column_type=[
        pickle.dumps(Int64),
        pickle.dumps(String),
        pickle.dumps(Bytes),
    ],
)
