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
    ModelOperation,
    PolicyOperation,
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
from mcp_server.client.plain.plain_rest_client_policy_operation import (
    PlainRESTClientPolicyOperation,
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


# pylint: disable=too-many-instance-attributes
class PlainRESTClientOperation(GravitinoOperation):
    def __init__(self, metalake_name: str, uri: str):
        rest_client = httpx.AsyncClient(base_url=uri)
        self.catalog_operation = PlainRESTClientCatalogOperation(
            metalake_name, rest_client
        )
        self.table_operation = PlainRESTClientTableOperation(
            metalake_name, rest_client
        )
        self.schema_operation = PlainRESTClientSchemaOperation(
            metalake_name, rest_client
        )
        self.topic_operation = PlainRESTClientTopicOperation(
            metalake_name, rest_client
        )
        self.model_operation = PlainRESTClientModelOperation(
            metalake_name, rest_client
        )
        self.tag_operation = PlainRESTClientTagOperation(
            metalake_name, rest_client
        )
        self.fileset_operation = PlainRESTClientFilesetOperation(
            metalake_name, rest_client
        )
        self.job_operation = PlainRESTClientJobOperation(
            metalake_name, rest_client
        )
        self.policy_operation = PlainRESTClientPolicyOperation(
            metalake_name, rest_client
        )

    def as_catalog_operation(self) -> CatalogOperation:
        return self.catalog_operation

    def as_table_operation(self) -> TableOperation:
        return self.table_operation

    def as_schema_operation(self) -> SchemaOperation:
        return self.schema_operation

    def as_topic_operation(self) -> TopicOperation:
        return self.topic_operation

    def as_model_operation(self) -> ModelOperation:
        return self.model_operation

    def as_fileset_operation(self):
        return self.fileset_operation

    def as_tag_operation(self) -> TagOperation:
        return self.tag_operation

    def as_job_operation(self) -> JobOperation:
        return self.job_operation

    def as_policy_operation(self) -> PolicyOperation:
        return self.policy_operation
