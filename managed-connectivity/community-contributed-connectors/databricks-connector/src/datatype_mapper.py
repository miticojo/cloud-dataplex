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

# Maps data types from Databricks to Dataplex Catalog

def get_catalog_metadata_type(data_type: str) -> str:
    """Choose the metadata type based on Databricks native type."""
    if data_type.upper() in ["BYTE", "TINYINT", "SHORT", "SMALLINT", "INT", "INTEGER", "LONG", "BIGINT", "FLOAT", "REAL", "DOUBLE", "DECIMAL", "DEC", "NUMERIC"]:
        return "NUMBER"
    if data_type.upper() in ["STRING", "VARCHAR", "CHAR"]:
        return "STRING"
    if data_type.upper() in ["TIMESTAMP", "DATE"]:
        return "TIMESTAMP"
    if data_type.upper() == "BOOLEAN":
        return "BOOLEAN"
    if data_type.upper() == "BINARY":
        return "BINARY"
    return "OTHER"