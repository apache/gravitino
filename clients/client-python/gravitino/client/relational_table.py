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
from gravitino.dto.rel.partitions.partition_dto import PartitionDTO
from gravitino.dto.rel.table_dto import TableDTO
from gravitino.dto.requests.add_partitions_request import AddPartitionsRequest
from gravitino.dto.responses.drop_response import DropResponse
from gravitino.dto.responses.partition_list_response import PartitionListResponse
from gravitino.dto.responses.partition_name_list_response import (
    PartitionNameListResponse,
)
from gravitino.dto.responses.partition_response import PartitionResponse
from gravitino.dto.util.dto_converters import DTOConverters
from gravitino.exceptions.handlers.partition_error_handler import (
    PARTITION_ERROR_HANDLER,
)
from gravitino.namespace import Namespace
from gravitino.rest.rest_utils import encode_string
from gravitino.utils import HTTPClient


class RelationalTable(Table):
    """Represents a relational table."""

    def __init__(
        self, namespace: Namespace, table_dto: TableDTO, rest_client: HTTPClient
    ):
        self._namespace = namespace
        self._table = cast(Table, DTOConverters.from_dto(table_dto))
        self._rest_client = rest_client

    def name(self) -> str:
        return self._table.name()

    def columns(self) -> list[Column]:
        metalake, catalog, schema = self._namespace.levels()
        return [
            cast(
                Column,
                GenericColumn(
                    c, self._rest_client, metalake, catalog, schema, self._table.name()
                ),
            )
            for c in self._table.columns()
        ]

    def partitioning(self) -> list[Transform]:
        return self._table.partitioning()

    def sort_order(self) -> list[SortOrder]:
        return self._table.sort_order()

    def distribution(self) -> Distribution:
        return self._table.distribution()

    def index(self) -> list[Index]:
        return self._table.index()

    def comment(self) -> Optional[str]:
        return self._table.comment()

    def properties(self) -> dict[str, str]:
        return self._table.properties()

    def audit_info(self) -> Audit:
        return self._table.audit_info()

    def _get_partition_request_path(self) -> str:
        """Get the partition request path.

        Returns:
            str: The partition request path.
        """

        return (
            f"api/metalakes/{encode_string(self._namespace.level(0))}"
            f"/catalogs/{encode_string(self._namespace.level(1))}"
            f"/schemas/{encode_string(self._namespace.level(2))}"
            f"/tables/{encode_string(self._table.name())}"
            "/partitions"
        )

    def list_partition_names(self) -> list[str]:
        """Get the partition names of the table.

        Returns:
            list[str]: The partition names of the table.
        """

        resp = self._rest_client.get(
            endpoint=self._get_partition_request_path(),
            error_handler=PARTITION_ERROR_HANDLER,
        )
        partition_name_list_resp = PartitionNameListResponse.from_json(
            resp.body, infer_missing=True
        )
        partition_name_list_resp.validate()

        return partition_name_list_resp.partition_names()

    def list_partitions(self) -> list[Partition]:
        """Get the partitions of the table.

        Returns:
            list[Partition]: The partitions of the table.
        """

        params = {"details": "true"}
        resp = self._rest_client.get(
            endpoint=self._get_partition_request_path(),
            params=params,
            error_handler=PARTITION_ERROR_HANDLER,
        )
        partition_list_resp = PartitionListResponse.from_json(
            resp.body, infer_missing=True
        )
        partition_list_resp.validate()

        return partition_list_resp.get_partitions()

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

        resp = self._rest_client.get(
            endpoint=f"{self._get_partition_request_path()}/{encode_string(partition_name)}",
            error_handler=PARTITION_ERROR_HANDLER,
        )
        partition_resp = PartitionResponse.from_json(resp.body, infer_missing=True)
        partition_resp.validate()

        return partition_resp.get_partition()

    def drop_partition(self, partition_name: str) -> bool:
        """Drops the partition with the given name.

        Args:
            partition_name (str): The name of the partition.

        Returns:
            bool: `True` if the partition is dropped, `False` if the partition does not exist.
        """
        resp = self._rest_client.delete(
            endpoint=f"{self._get_partition_request_path()}/{encode_string(partition_name)}",
            error_handler=PARTITION_ERROR_HANDLER,
        )
        drop_resp = DropResponse.from_json(resp.body, infer_missing=True)
        drop_resp.validate()

        return drop_resp.dropped()

    def add_partition(self, partition: Partition) -> Partition:
        """Adds a partition to the table.

        Args:
            partition (Partition): The partition to add.

        Returns:
            Partition: The added partition.

        Raises:
            PartitionAlreadyExistsException:
                if the partition already exists, throws this exception.
        """

        req = AddPartitionsRequest(
            [cast(PartitionDTO, DTOConverters.to_dto(partition))]
        )
        req.validate()

        resp = self._rest_client.post(
            endpoint=self._get_partition_request_path(),
            json=req,
            error_handler=PARTITION_ERROR_HANDLER,
        )
        partition_list_resp = PartitionListResponse.from_json(
            resp.body, infer_missing=True
        )
        partition_list_resp.validate()
        return partition_list_resp.get_partitions()[0]
