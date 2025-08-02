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

import httpx
from mcp_server.connector import CatalogOperation, GravitinoOperation, \
    SchemaOperation, TableOperation
from mcp_server.connector.rest.rest_client_catalog_operation import \
    RESTClientCatalogOperation
from mcp_server.connector.rest.rest_client_schema_operation import \
    RESTClientSchemaOperation
from mcp_server.connector.rest.rest_client_table_operation import \
    RESTClientTableOperation


class RESTClientOperation(GravitinoOperation):
    def __init__(self, metalake_name: str, uri: str):
        self.metalake_name = metalake_name
        self.rest_client = httpx.Client(base_url=uri)

    def as_catalog_operation(self) -> CatalogOperation:
        return RESTClientCatalogOperation(
            metalake_name=self.metalake_name, rest_client=self.rest_client
        )

    def as_table_operation(self) -> TableOperation:
        return RESTClientTableOperation(
            metalake_name=self.metalake_name, rest_client=self.rest_client
        )

    def as_schema_operation(self) -> SchemaOperation:
        return RESTClientSchemaOperation(
            metalake_name=self.metalake_name, rest_client=self.rest_client
        )
