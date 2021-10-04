import pickle

from src.tanuki.data_store.data_type import Boolean, Bytes, Int64, String
from src.tanuki.database.reference_tables import (
    IndexReference,
    PROTECTED_GROUP,
    StoreDefinition,
    StoreReference,
    TableReference,
)

TABLE_REFERENCE = TableReference(
    table_name=[
        "table_reference",
        "store_reference",
        "index_reference",
        "StoreDefinition_v1_definition",
        "TableReference_v1_definition",
        "StoreReference_v1_definition",
        "IndexReference_v1_definition",
    ],
    data_group=[
        PROTECTED_GROUP,
        PROTECTED_GROUP,
        PROTECTED_GROUP,
        PROTECTED_GROUP,
        PROTECTED_GROUP,
        PROTECTED_GROUP,
        PROTECTED_GROUP,
    ],
    store_type=[
        "TableReference",
        "StoreReference",
        "IndexReference",
        "StoreDefinition",
        "StoreDefinition",
        "StoreDefinition",
        "StoreDefinition",
    ],
    store_version=[1, 1, 1, 1, 1, 1, 1],
    protected=[True, True, True, True, True, True, True],
)

STORE_REFERENCE = StoreReference(
    store_type=[
        "StoreDefinition",
        "TableReference",
        "StoreReference",
        "IndexReference",
    ],
    store_version=[1, 1, 1, 1],
    definition_reference=[
        "StoreDefinition_v1_definition",
        "TableReference_v1_definition",
        "StoreReference_v1_definition",
        "IndexReference_v1_definition",
    ],
    definition_version=[1, 1, 1, 1],
)


INDEX_REFERENCE = IndexReference(
    store_type=[
        "StoreDefinition",
        "TableReference",
        "TableReference",
        "StoreReference",
        "StoreReference",
        "IndexReference",
        "IndexReference",
        "IndexReference",
        "IndexReference",
    ],
    store_version=[1, 1, 1, 1, 1, 1, 1, 1, 1],
    index_name=[
        "name_index",
        "table_group_index",
        "table_group_index",
        "type_version_index",
        "type_version_index",
        "complete_index",
        "complete_index",
        "complete_index",
        "complete_index",
    ],
    column_name=[
        "column_name",
        "table_name",
        "data_group",
        "store_type",
        "store_version",
        "store_type",
        "store_version",
        "index_name",
        "column_name",
    ],
)

TABLE_REFERENCE_STORE_DEFINITION = StoreDefinition(
    column_name=[
        "table_name",
        "data_group",
        "store_type",
        "store_version",
        "protected",
    ],
    column_type=[
        pickle.dumps(String),
        pickle.dumps(String),
        pickle.dumps(String),
        pickle.dumps(Int64),
        pickle.dumps(Boolean),
    ],
)

STORE_REFERENCE_STORE_DEFINITION = StoreDefinition(
    column_name=[
        "store_type",
        "store_version",
        "definition_reference",
        "definition_version",
    ],
    column_type=[
        pickle.dumps(String),
        pickle.dumps(Int64),
        pickle.dumps(String),
        pickle.dumps(Int64),
    ],
)

STORE_DEFINITION_STORE_DEFINITION = StoreDefinition(
    column_name=[
        "column_name",
        "column_type",
    ],
    column_type=[
        pickle.dumps(String),
        pickle.dumps(Bytes),
    ],
)

INDEX_REFERENCE_STORE_DEFINITION = StoreDefinition(
    column_name=[
        "store_type",
        "store_version",
        "index_name",
        "column_name",
    ],
    column_type=[
        pickle.dumps(String),
        pickle.dumps(Int64),
        pickle.dumps(String),
        pickle.dumps(String),
    ],
)
