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

from dataclasses_json import DataClassJsonMixin, config

from gravitino.api.rel.expressions.distributions.distribution import Distribution
from gravitino.api.rel.expressions.transforms.transform import Transform
from gravitino.api.rel.indexes.index import Index
from gravitino.api.rel.table import Table
from gravitino.dto.audit_dto import AuditDTO
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
from gravitino.utils.precondition import Precondition


@dataclass
class TableDTO(Table, DataClassJsonMixin):  # pylint: disable=R0902
    """Represents a Table DTO (Data Transfer Object)."""

    _name: Optional[str] = field(default=None, metadata=config(field_name="name"))
    _columns: Optional[list[ColumnDTO]] = field(
        default=None, metadata=config(field_name="columns")
    )
    _audit: Optional[AuditDTO] = field(
        default=None, metadata=config(field_name="audit")
    )
    _comment: Optional[str] = field(
        default=None,
        metadata=config(field_name="comment", exclude=lambda value: value is None),
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

    def __post_init__(self):
        Precondition.check_argument(
            self._name is not None and self._name != "",
            "name cannot be null or empty",
        )
        Precondition.check_argument(self._audit is not None, "audit cannot be null")
        Precondition.check_argument(
            self._columns is not None and len(self._columns) > 0,
            "columns cannot be null or empty",
        )

    def name(self) -> str:
        return self._name

    def comment(self) -> Optional[str]:
        return self._comment

    def columns(self) -> list[ColumnDTO]:
        return self._columns

    def audit_info(self) -> AuditDTO:
        return self._audit

    def properties(self) -> Optional[dict[str, str]]:
        return self._properties

    def partitioning(self) -> Optional[list[Transform]]:
        return self._partitioning

    def sort_order(self) -> Optional[list[SortOrderDTO]]:
        return self._sort_orders

    def distribution(self) -> Optional[Distribution]:
        return self._distribution

    def index(self) -> Optional[list[Index]]:
        return self._indexes
