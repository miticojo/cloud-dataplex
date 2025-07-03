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

from typing import Dict
from pyspark.sql import SparkSession, DataFrame
from src.constants import EntryType
from src.common.connection_jar import getJarPath
from src.common.util import fileExists
from src.constants import JDBC_JAR
from src.common.secret_manager import get_secret

class DatabricksConnector:
    """Reads data from Databricks and returns Spark Dataframes."""

    def __init__(self, config: Dict[str, str]):
        # PySpark entrypoint

        # Get jar file, allowing override for local jar file (different version / name)
        jar_path = getJarPath(config,[JDBC_JAR])
        # Check jar files exist. Throws exception if not found
        jarsExist = fileExists(jar_path)

        self._spark = SparkSession.builder.appName("DatabricksIngestor") \
            .config("spark.jars",jar_path) \
            .config("spark.log.level", "ERROR") \
            .getOrCreate()

        access_token = get_secret(config['access_token_secret'])

        self._url = f"jdbc:databricks://{config['server_hostname']};" \
                    f"HttpPath={config['http_path']};" \
                    f"AuthMech=3;" \
                    f"UID=token;" \
                    f"PWD={access_token}"

    def _execute(self, query: str) -> DataFrame:
        return self._spark.read \
            .format("jdbc") \
            .option("url", self._url) \
            .option("query", query) \
            .load()

    def get_db_catalogs(self) -> DataFrame:
        query = """
        SELECT catalog_name FROM information_schema.catalogs
        """
        return self._execute(query)

    def get_db_schemas(self, catalog_name: str) -> DataFrame:
        query = f"""
        SELECT schema_name FROM {catalog_name}.information_schema.schemata
        WHERE schema_name != 'information_schema'
        """
        return self._execute(query)

    def _get_columns(self, catalog_name: str, schema_name: str, object_type: str) -> str:
        """Returns list of columns a tables or view"""
        return (f"SELECT c.table_name, c.column_name,  "
                f"c.data_type, c.is_nullable "
                f"FROM {catalog_name}.information_schema.columns c "
                f"JOIN {catalog_name}.information_schema.tables t ON  "
                f"c.table_catalog = t.table_catalog "
                f"AND c.table_schema = t.table_schema "
                f"AND c.table_name = t.table_name "
                f"WHERE c.table_schema = '{schema_name}' "
                f"AND t.table_type = '{object_type}'")

    def get_dataset(self, catalog_name: str, schema_name: str, entry_type: EntryType):
        """Gets data for a table or a view."""
        short_type = entry_type.name  # table or view, or the title of enum value
        if ( short_type == "TABLE" ):
            object_type = "BASE TABLE"
        else:
            object_type = "VIEW"
        query = self._get_columns(catalog_name, schema_name, object_type)
        return self._execute(query)