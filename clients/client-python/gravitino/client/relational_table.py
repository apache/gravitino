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

from typing import Optional, cast

from gravitino.api.audit import Audit
from gravitino.api.rel.column import Column
from gravitino.api.rel.expressions.distributions.distribution import Distribution
from gravitino.api.rel.expressions.sorts.sort_order import SortOrder
from gravitino.api.rel.expressions.transforms.transform import Transform
from gravitino.api.rel.indexes.index import Index
from gravitino.api.rel.partitions.partition import Partition
from gravitino.api.rel.table import Table
from gravitino.client.generic_column import GenericColumn
from gravitino.dto.responses.partition_list_response import PartitionListResponse
from gravitino.dto.responses.partition_name_list_response import (
    PartitionNameListResponse,
)
from gravitino.dto.responses.partition_response import PartitionResponse
from gravitino.exceptions.handlers.partition_error_handler import (
    PARTITION_ERROR_HANDLER,
)
from gravitino.namespace import Namespace
from gravitino.rest.rest_utils import encode_string
from gravitino.utils import HTTPClient


class RelationalTable(Table):  # pylint: disable=too-many-instance-attributes
    """Represents a relational table."""

    def __init__(
        self,
        name: str,
        columns: list[Column],
        partitioning: list[Transform],
        sort_order: list[SortOrder],
        distribution: Distribution,
        index: list[Index],
        comment: Optional[str],
        properties: dict[str, str],
        audit_info: Audit,
        namespace: Namespace,
        rest_client: HTTPClient,
    ):
        self._name = name
        self._columns = columns
        self._partitioning = partitioning
        self._sort_order = sort_order
        self._distribution = distribution
        self._index = index
        self._comment = comment
        self._properties = properties
        self._audit_info = audit_info
        self._namespace = namespace
        self._rest_client = rest_client

    def name(self) -> str:
        return self._name

    def columns(self) -> list[Column]:
        metalake, catalog, schema = self._namespace.levels()
        return [
            cast(
                Column,
                GenericColumn(
                    c, self._rest_client, metalake, catalog, schema, self._name
                ),
            )
            for c in self._columns
        ]

    def partitioning(self) -> list[Transform]:
        return self._partitioning

    def sort_order(self) -> list[SortOrder]:
        return self._sort_order

    def distribution(self) -> Distribution:
        return self._distribution

    def index(self) -> list[Index]:
        return self._index

    def comment(self) -> Optional[str]:
        return self._comment

    def properties(self) -> dict[str, str]:
        return self._properties

    def audit_info(self) -> Audit:
        return self._audit_info

    def get_partition_request_path(self) -> str:
        """Get the partition request path.

        Returns:
            str: The partition request path.
        """

        return (
            f"api/metalakes/{encode_string(self._namespace.level(0))}"
            f"/catalogs/{encode_string(self._namespace.level(1))}"
            f"/schemas/{encode_string(self._namespace.level(2))}"
            f"/tables/{encode_string(self._name)}"
            "/partitions/"
        )

    def list_partition_names(self) -> list[str]:
        """Get the partition names of the table.

        Returns:
            list[str]: The partition names of the table.
        """

        resp = cast(
            PartitionNameListResponse,
            self._rest_client.get(
                endpoint=self.get_partition_request_path(),
                error_handler=PARTITION_ERROR_HANDLER,
            ),
        )
        return resp.partition_names()

    def list_partitions(self) -> list[Partition]:
        """Get the partitions of the table.

        Returns:
            list[Partition]: The partitions of the table.
        """

        params = {"details": "true"}
        resp = cast(
            PartitionListResponse,
            self._rest_client.get(
                endpoint=self.get_partition_request_path(),
                params=params,
                error_handler=PARTITION_ERROR_HANDLER,
            ),
        )
        return resp.get_partitions()

    def get_partition(self, partition_name: str) -> Partition:
        """Returns the partition with the given name.

        Args:
            partition_name (str): the name of the partition

        Returns:
            Partition: the partition with the given name

        Raises:
            NoSuchPartitionException:
                if the partition does not exist, throws this exception.
        """

        resp = cast(
            PartitionResponse,
            self._rest_client.get(
                endpoint=f"{self.get_partition_request_path()}/{partition_name}",
                error_handler=PARTITION_ERROR_HANDLER,
            ),
        )
        return resp.get_partition()
