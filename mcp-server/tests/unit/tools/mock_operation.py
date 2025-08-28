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
    PolicyOperation,
    SchemaOperation,
    TableOperation,
    TagOperation,
    TopicOperation,
)
from mcp_server.client.fileset_operation import FilesetOperation
from mcp_server.client.job_operation import JobOperation
from mcp_server.client.statistic_operation import StatisticOperation


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

    def as_job_operation(self) -> JobOperation:
        return MockJobOperation()

    def as_statistic_operation(self) -> StatisticOperation:
        return MockStatisticOperation()

    def as_policy_operation(self) -> PolicyOperation:
        return MockPolicyOperation()


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
    async def list_of_filesets(
        self, catalog_name: str, schema_name: str
    ) -> str:
        return "mock_filesets"

    async def load_fileset(
        self, catalog_name: str, schema_name: str, fileset_name: str
    ) -> str:
        return "mock_fileset"

    # pylint: disable=R0917
    # This method has too many arguments, but it's a mock and we want to keep the signature similar to the real one.
    async def list_files_in_fileset(
        self,
        catalog_name: str,
        schema_name: str,
        fileset_name: str,
        location_name: str,
        sub_path: str = "/",
    ) -> str:
        return "mock_files_in_fileset"


class MockPolicyOperation(PolicyOperation):
    async def associate_policy_with_metadata(
        self,
        metadata_full_name: str,
        metadata_type: str,
        policies_to_add: list,
        policies_to_remove: list,
    ) -> str:
        return (
            f"associate_policy_with_metadata: {metadata_full_name}, {metadata_type}, "
            f"{policies_to_add}, {policies_to_remove}"
        )

    async def get_policy_for_metadata(
        self, metadata_full_name: str, metadata_type: str, policy_name: str
    ) -> str:
        return f"get_policy_for_metadata: {metadata_full_name}, {metadata_type}, {policy_name}"

    async def list_policies_for_metadata(
        self, metadata_full_name: str, metadata_type: str
    ) -> str:
        return (
            f"list_policies_for_metadata: {metadata_full_name}, {metadata_type}"
        )

    async def list_metadata_by_policy(self, policy_name: str) -> str:
        return f"list_metadata_by_policy: {policy_name}"

    async def get_list_of_policies(self) -> str:
        return "mock_policies"

    async def load_policy(self, policy_name: str) -> str:
        return f"mock_policy: {policy_name}"


class MockModelOperation(ModelOperation):
    async def list_of_models(self, catalog_name: str, schema_name: str) -> str:
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
    async def list_of_topics(self, catalog_name: str, schema_name: str) -> str:
        return "mock_topics"

    async def load_topic(
        self, catalog_name: str, schema_name: str, topic_name: str
    ) -> str:
        return "mock_topic"


class MockTagOperation(TagOperation):
    async def list_of_tags(self) -> str:
        return "mock_tags"

    async def create_tag(
        self, tag_name: str, tag_comment: str, tag_properties: dict
    ) -> str:
        return f"mock_tag_created: {tag_name}"

    async def get_tag_by_name(self, tag_name: str) -> str:
        return f"mock_tag: {tag_name}"

    async def alter_tag(self, tag_name: str, updates: list) -> str:
        return f"mock_tag_altered: {tag_name} with updates {updates}"

    async def delete_tag(self, name: str) -> str:
        return f"mock_tag_deleted: {name}"

    async def associate_tag_with_metadata(
        self,
        metadata_full_name: str,
        metadata_type: str,
        tags_to_associate: list,
        tags_to_disassociate: list,
    ) -> str:
        return f"mock_associated_tags: {tags_to_associate} with metadata {metadata_full_name} of type {metadata_type}"

    async def list_tags_for_metadata(
        self, metadata_full_name: str, metadata_type: str
    ) -> str:
        return f"mock_tags_for_metadata: {metadata_full_name} of type {metadata_type}"

    async def list_metadata_by_tag(self, tag_name: str) -> str:
        return f"mock_metadata_by_tag: {tag_name}"


class MockJobOperation(JobOperation):
    async def list_of_jobs(self, job_template_name: str = "") -> str:
        return "mock_jobs"

    async def get_job_by_id(self, job_id: str) -> str:
        return f"mock_job: {job_id}"

    async def list_of_job_templates(self) -> str:
        return "mock_job_templates"

    async def get_job_template_by_name(self, name: str) -> str:
        return f"mock_job_template: {name}"

    async def run_job(self, job_template_name: str, job_config: dict) -> str:
        return f"mock_job_run: {job_template_name} with parameters {job_config}"

    async def cancel_job(self, job_id: str) -> str:
        return f"mock_job_cancelled: {job_id}"


class MockStatisticOperation(StatisticOperation):
    async def list_of_statistics(
        self, metalake_name: str, metadata_type: str, metadata_fullname: str
    ) -> str:
        return f"mock_statistics: {metalake_name}, {metadata_type}, {metadata_fullname}"

    # pylint: disable=R0917
    async def list_statistic_for_partition(
        self,
        metalake_name: str,
        metadata_type: str,
        metadata_fullname: str,
        from_partition_name: str,
        to_partition_name: str,
        from_inclusive: bool = True,
        to_inclusive: bool = False,
    ) -> str:
        return (
            f"mock_statistics_for_partition: {metalake_name}, {metadata_type}, {metadata_fullname},"
            f" {from_partition_name}, {to_partition_name}, {from_inclusive}, {to_inclusive}"
        )
