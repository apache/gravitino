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

from mcp_server.client import (
    CatalogOperation,
    GravitinoOperation,
    SchemaOperation,
    TableOperation,
    TagOperation,
)
from mcp_server.client.job_operation import JobOperation
from mcp_server.client.plain.plain_rest_client_catalog_operation import (
    PlainRESTClientCatalogOperation,
)
from mcp_server.client.plain.plain_rest_client_fileset_operation import (
    PlainRESTClientFilesetOperation,
)
from mcp_server.client.plain.plain_rest_client_job_operation import (
    PlainRESTClientJobOperation,
)
from mcp_server.client.plain.plain_rest_client_model_operation import (
    PlainRESTClientModelOperation,
)
from mcp_server.client.plain.plain_rest_client_schema_operation import (
    PlainRESTClientSchemaOperation,
)
from mcp_server.client.plain.plain_rest_client_table_operation import (
    PlainRESTClientTableOperation,
)
from mcp_server.client.plain.plain_rest_client_tag_operation import (
    PlainRESTClientTagOperation,
)
from mcp_server.client.plain.plain_rest_client_topic_operation import (
    PlainRESTClientTopicOperation,
)
from mcp_server.client.topic_operation import TopicOperation


class PlainRESTClientOperation(GravitinoOperation):
    def __init__(self, metalake_name: str, uri: str):
        self.metalake_name = metalake_name
        self.rest_client = httpx.AsyncClient(base_url=uri)

    def as_catalog_operation(self) -> CatalogOperation:
        return PlainRESTClientCatalogOperation(
            metalake_name=self.metalake_name, rest_client=self.rest_client
        )

    def as_table_operation(self) -> TableOperation:
        return PlainRESTClientTableOperation(
            metalake_name=self.metalake_name, rest_client=self.rest_client
        )

    def as_schema_operation(self) -> SchemaOperation:
        return PlainRESTClientSchemaOperation(
            metalake_name=self.metalake_name, rest_client=self.rest_client
        )

    def as_topic_operation(self) -> TopicOperation:
        return PlainRESTClientTopicOperation(
            metalake_name=self.metalake_name, rest_client=self.rest_client
        )

    def as_model_operation(self):
        return PlainRESTClientModelOperation(
            metalake_name=self.metalake_name, rest_client=self.rest_client
        )

    def as_fileset_operation(self):
        return PlainRESTClientFilesetOperation(
            metalake_name=self.metalake_name, rest_client=self.rest_client
        )

    def as_tag_operation(self) -> TagOperation:
        return PlainRESTClientTagOperation(
            metalake_name=self.metalake_name, rest_client=self.rest_client
        )

    def as_job_operation(self) -> JobOperation:
        return PlainRESTClientJobOperation(
            metalake_name=self.metalake_name, rest_client=self.rest_client
        )
