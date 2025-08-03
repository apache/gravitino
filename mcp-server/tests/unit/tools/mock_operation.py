# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from mcp_server.connector import (
    CatalogOperation,
    GravitinoOperation,
    SchemaOperation,
    TableOperation,
)


class MockOperation(GravitinoOperation):
    def __init__(self, metalake, uri):
        pass

    def as_table_operation(self) -> TableOperation:
        return MockTableOperation()

    def as_schema_operation(self) -> SchemaOperation:
        return MockSchemaOperation()

    def as_catalog_operation(self) -> CatalogOperation:
        return MockCatalogOperation()


class MockCatalogOperation(CatalogOperation):
    def get_list_of_catalogs(self) -> str:
        return "mock_catalogs"


class MockSchemaOperation(SchemaOperation):
    def get_list_of_schemas(self, catalog_name: str) -> str:
        return "mock_schemas"


class MockTableOperation(TableOperation):
    def get_list_of_tables(self, catalog_name: str, schema_name: str) -> str:
        return "mock_tables"

    def load_table(
        self, catalog_name: str, schema_name: str, table_name: str
    ) -> str:
        return "mock_table"
