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

from mcp_server.client import (
    CatalogOperation,
    GravitinoOperation,
    SchemaOperation,
    TableOperation,
)
from mcp_server.client.job_operation import JobOperation
from mcp_server.client.tag_operation import TagOperation


class MockOperation(GravitinoOperation):
    def __init__(self, metalake, uri):
        pass

    def as_table_operation(self) -> TableOperation:
        return MockTableOperation()

    def as_schema_operation(self) -> SchemaOperation:
        return MockSchemaOperation()

    def as_catalog_operation(self) -> CatalogOperation:
        return MockCatalogOperation()

    def as_tag_operation(self) -> TagOperation:
        return MockTagOperation()

    def as_job_operation(self):
        return MockJobOperation()


class MockCatalogOperation(CatalogOperation):
    async def get_list_of_catalogs(self) -> str:
        return "mock_catalogs"


class MockSchemaOperation(SchemaOperation):
    async def get_list_of_schemas(self, catalog_name: str) -> str:
        return "mock_schemas"


class MockTableOperation(TableOperation):
    async def get_list_of_tables(
        self, catalog_name: str, schema_name: str
    ) -> str:
        return "mock_tables"

    async def load_table(
        self, catalog_name: str, schema_name: str, table_name: str
    ) -> str:
        return "mock_table"


class MockTagOperation(TagOperation):
    async def get_list_of_tags(self) -> str:
        return "mock_tags"

    async def create_tag(
        self, name: str, comment: str, properties: dict
    ) -> str:
        return f"mock_tag_created: {name}"

    async def get_tag_by_name(self, name: str) -> str:
        return f"mock_tag: {name}"

    async def alter_tag(self, name: str, updates: list) -> str:
        return f"mock_tag_altered: {name} with updates {updates}"

    async def delete_tag(self, name: str) -> str:
        return f"mock_tag_deleted: {name}"

    async def associate_tag_with_metadata(
        self,
        metadata_full_name: str,
        metadata_type: str,
        tags_to_associate: list,
        tags_to_disassociate,
    ) -> str:
        return f"mock_associated_tags: {tags_to_associate} with metadata {metadata_full_name} of type {metadata_type}"

    async def list_tags_for_metadata(
        self, metadata_full_name: str, metadata_type: str
    ) -> str:
        return f"mock_tags_for_metadata: {metadata_full_name} of type {metadata_type}"

    async def list_metadata_by_tag(self, tag_name: str) -> str:
        return f"mock_metadata_by_tag: {tag_name}"


class MockJobOperation(JobOperation):
    async def get_list_of_jobs(self, job_template_name: str = "") -> str:
        return "mock_jobs"

    async def get_job_by_id(self, job_id: str) -> str:
        return f"mock_job: {job_id}"

    async def get_list_of_job_templates(self) -> str:
        return "mock_job_templates"

    async def get_job_template_by_name(self, name: str) -> str:
        return f"mock_job_template: {name}"
