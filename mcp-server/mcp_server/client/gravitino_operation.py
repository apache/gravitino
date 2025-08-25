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

from abc import ABC, abstractmethod

from mcp_server.client.catalog_operation import CatalogOperation
from mcp_server.client.fileset_operation import FilesetOperation
from mcp_server.client.job_operation import JobOperation
from mcp_server.client.model_operation import ModelOperation
from mcp_server.client.policy_operation import PolicyOperation
from mcp_server.client.schema_operation import SchemaOperation
from mcp_server.client.statistic_operation import StatisticOperation
from mcp_server.client.table_operation import TableOperation
from mcp_server.client.tag_operation import TagOperation
from mcp_server.client.topic_operation import TopicOperation


class GravitinoOperation(ABC):
    """
    Abstract base class for Gravitino operations with multiple operation facets.
    """

    @abstractmethod
    def as_table_operation(self) -> TableOperation:
        """
        Access the table operation interface of this Gravitino operation.

        Returns:
            TableOperation: Interface for performing table-level operations
        """
        pass

    @abstractmethod
    def as_schema_operation(self) -> SchemaOperation:
        """
        Access the schema operation interface of this Gravitino operation.

        Returns:
            SchemaOperation: Interface for performing schema-level operations
        """
        pass

    @abstractmethod
    def as_catalog_operation(self) -> CatalogOperation:
        """
        Access the catalog operation interface of this Gravitino operation.

        Returns:
            CatalogOperation: Interface for performing catalog-level operations
        """
        pass

    @abstractmethod
    def as_topic_operation(self) -> TopicOperation:
        """
        Access the topic operation interface of this Gravitino operation.

        Returns:
            TopicOperation: Interface for performing topic-level operations
        """
        pass

    @abstractmethod
    def as_model_operation(self) -> ModelOperation:
        """
        Access the model operation interface of this Gravitino operation.

        Returns:
            ModelOperation: Interface for performing model-level operations
        """
        pass

    @abstractmethod
    def as_policy_operation(self) -> PolicyOperation:
        """
        Access the policy operation interface of this Gravitino operation.

        Returns:
            PolicyOperation: Interface for performing policy-level operations
        """
        pass

    @abstractmethod
    def as_fileset_operation(self) -> FilesetOperation:
        """
        Access the fileset operation interface of this Gravitino operation.

        Returns:
            FilesetOperation: Interface for performing fileset-level operations
        pass
        """

    def as_tag_operation(self) -> TagOperation:
        """
        Access the tag operation interface of this Gravitino operation.

        Returns:
            TagOperation: Interface for performing tag-level operations
        """
        pass

    @abstractmethod
    def as_job_operation(self) -> JobOperation:
        """
        Access the job operation interface of this Gravitino operation.

        Returns:
            JobOperation: Interface for performing job-level operations
        """
        pass

    @abstractmethod
    def as_statistic_operation(self) -> StatisticOperation:
        """
        Access the statistic operation interface of this Gravitino operation.

        Returns:
            StatisticOperation: Interface for performing statistic-level operations
        """
        pass
