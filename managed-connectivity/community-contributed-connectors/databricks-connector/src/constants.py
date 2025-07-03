# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

## Databricks specific constants and functions

import enum
from typing import List

SOURCE_TYPE = "databricks"

# Default JDBC and Databricks Spark JAR file.
JDBC_JAR = "DatabricksJDBC42.jar"

# Allow common bootstrap to load connector for specific datasource
CONNECTOR_MODULE = "src.databricks_connector"
CONNECTOR_CLASS = "DatabricksConnector"

# Value to test for if column is nullable. Databricks specific.
# Matches value in is_nullable column from _get_columns
IS_NULLABLE_TRUE = "YES"

class EntryType(enum.Enum):
    """Hierarchy of Databricks entries"""
    CATALOG: str = "projects/{project}/locations/{location}/entryTypes/databricks-catalog"
    DB_SCHEMA: str = "projects/{project}/locations/{location}/entryTypes/databricks-schema"
    TABLE: str = "projects/{project}/locations/{location}/entryTypes/databricks-table"
    VIEW: str = "projects/{project}/locations/{location}/entryTypes/databricks-view"

# Top-level types in EntryType hierarchy which will be written to file before schema processing starts
TOP_ENTRY_HIERARCHY : List[EntryType] = [EntryType.CATALOG]

# EntryType in the hierarchy under which database objects like tables, views are organised and processed
COLLECTION_ENTRY : EntryType = EntryType.DB_SCHEMA

# DB objects to extract metadata for
DB_OBJECT_TYPES_TO_PROCESS : List[EntryType] = [EntryType.TABLE, EntryType.VIEW]

# metadata file name
def generateFileName(config: dict[str:str]) -> str:
    return f"{SOURCE_TYPE}-{config['server_hostname']}.jsonl"