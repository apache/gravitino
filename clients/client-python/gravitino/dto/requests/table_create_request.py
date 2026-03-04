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

from dataclasses import dataclass, field
from typing import Optional

from dataclasses_json import config

from gravitino.dto.rel.column_dto import ColumnDTO
from gravitino.dto.rel.distribution_dto import DistributionDTO
from gravitino.dto.rel.indexes.index_dto import IndexDTO
from gravitino.dto.rel.indexes.json_serdes.index_serdes import IndexSerdes
from gravitino.dto.rel.json_serdes.distribution_serdes import DistributionSerDes
from gravitino.dto.rel.json_serdes.sort_order_serdes import SortOrderSerdes
from gravitino.dto.rel.partitioning.json_serdes.partitioning_serdes import (
    PartitioningSerdes,
)
from gravitino.dto.rel.partitioning.partitioning import Partitioning
from gravitino.dto.rel.sort_order_dto import SortOrderDTO
from gravitino.rest.rest_message import RESTRequest
from gravitino.utils.precondition import Precondition


@dataclass
class TableCreateRequest(RESTRequest):  # pylint: disable=too-many-instance-attributes
    """Represents a request to create a table."""

    _name: str = field(metadata=config(field_name="name"))
    _columns: list[ColumnDTO] = field(metadata=config(field_name="columns"))
    _comment: Optional[str] = field(default=None, metadata=config(field_name="comment"))
    _partitioning: Optional[list[Partitioning]] = field(
        default=None,
        metadata=config(
            field_name="partitioning",
            encoder=lambda items: [
                PartitioningSerdes.serialize(item) for item in items
            ],
            decoder=lambda values: [
                PartitioningSerdes.deserialize(value) for value in values
            ],
            exclude=lambda value: value is None,
        ),
    )
    _distribution: Optional[DistributionDTO] = field(
        default=None,
        metadata=config(
            field_name="distribution",
            encoder=DistributionSerDes.serialize,
            decoder=DistributionSerDes.deserialize,
            exclude=lambda value: value is None,
        ),
    )
    _sort_orders: Optional[list[SortOrderDTO]] = field(
        default=None,
        metadata=config(
            field_name="sortOrders",
            encoder=lambda items: [SortOrderSerdes.serialize(item) for item in items],
            decoder=lambda values: [
                SortOrderSerdes.deserialize(value) for value in values
            ],
            exclude=lambda value: value is None,
        ),
    )
    _indexes: Optional[list[IndexDTO]] = field(
        default=None,
        metadata=config(
            field_name="indexes",
            encoder=lambda items: [IndexSerdes.serialize(item) for item in items],
            decoder=lambda values: [IndexSerdes.deserialize(value) for value in values],
            exclude=lambda value: value is None,
        ),
    )
    _properties: Optional[dict[str, str]] = field(
        default=None,
        metadata=config(field_name="properties", exclude=lambda value: value is None),
    )

    def validate(self):
        Precondition.check_string_not_empty(
            self._name, '"name" field is required and cannot be empty'
        )
        if self._sort_orders:
            for sort_order in self._sort_orders:
                sort_order.validate(self._columns)

        if self._distribution:
            for expression in self._distribution.expressions():
                expression.validate(self._columns)

        if self._partitioning:
            for partitioning in self._partitioning:
                partitioning.validate(self._columns)

        for column in self._columns:
            column.validate()
        auto_increment_cols = [
            column for column in self._columns if column.auto_increment()
        ]
        auto_increment_cols_str = (
            f"[{','.join(column.name() for column in auto_increment_cols)}]"
        )
        Precondition.check_argument(
            len(auto_increment_cols) <= 1,
            "Only one column can be auto-incremented. There are multiple auto-increment "
            f"columns in your table: {auto_increment_cols_str}",
        )

        if self._indexes:
            for index in self._indexes:
                Precondition.check_argument(
                    index.type() is not None, "Index type cannot be null"
                )
                Precondition.check_argument(
                    index.field_names() is not None, "Index fieldNames cannot be null"
                )
                Precondition.check_argument(
                    len(index.field_names()) > 0,
                    "Index fieldNames length must be greater than 0",
                )
