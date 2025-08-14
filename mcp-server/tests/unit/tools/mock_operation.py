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
    ModelOperation,
    SchemaOperation,
    TableOperation,
    TopicOperation,
)
from mcp_server.client.fileset_operation import FilesetOperation
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

    def as_fileset_operation(self) -> FilesetOperation:
        return MockFilesetOperation()

    def as_topic_operation(self) -> TopicOperation:
        return MockTopicOperation()

    def as_model_operation(self) -> ModelOperation:
        return MockModelOperation()

    def as_tag_operation(self) -> TagOperation:
        return MockTagOperation()


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


class MockFilesetOperation(FilesetOperation):
    async def get_list_of_filesets(
        self, catalog_name: str, schema_name: str
    ) -> str:
        return "mock_filesets"

    async def load_fileset(
        self, catalog_name: str, schema_name: str, fileset_name: str
    ) -> str:
        return "mock_fileset"

    async def list_files_in_fileset(
        self,
        catalog_name: str,
        schema_name: str,
        fileset_name: str,
        location_name: str,
        sub_path: str = "/",
    ) -> str:
        return "mock_files_in_fileset"


class MockModelOperation(ModelOperation):
    async def get_list_of_models(
        self, catalog_name: str, schema_name: str
    ) -> str:
        return "mock_models"

    async def load_model(
        self, catalog_name: str, schema_name: str, model_name: str
    ) -> str:
        return "mock_model"

    async def list_model_versions(
        self, catalog_name: str, schema_name: str, model_name: str
    ) -> str:
        return "mock_model_versions"

    async def load_model_version(
        self, catalog_name: str, schema_name: str, model_name: str, version: int
    ) -> str:
        return "mock_model_version"

    async def load_model_version_by_alias(
        self, catalog_name: str, schema_name: str, model_name: str, alias: str
    ) -> str:
        return "mock_model_version_by_alias"


class MockTopicOperation(TopicOperation):
    async def get_list_of_topics(
        self, catalog_name: str, schema_name: str
    ) -> str:
        return "mock_topics"

    async def load_topic(
        self, catalog_name: str, schema_name: str, topic_name: str
    ) -> str:
        return "mock_topic"


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
