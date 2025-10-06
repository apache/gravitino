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
from marshmallow.fields import Field

from gravitino.api.rel.expressions.distributions.distribution import Distribution
from gravitino.api.rel.expressions.transforms.transform import Transform
from gravitino.api.rel.indexes.index import Index
from gravitino.api.rel.table import Table
from gravitino.dto.audit_dto import AuditDTO
from gravitino.dto.rel.column_dto import ColumnDTO
from gravitino.dto.rel.distribution_dto import DistributionDTO
from gravitino.dto.rel.indexes.index_dto import IndexDTO
from gravitino.dto.rel.partitioning.partitioning import Partitioning
from gravitino.dto.rel.sort_order_dto import SortOrderDTO
from gravitino.utils.precondition import Precondition


@dataclass
class TableDTO(Table, DataClassJsonMixin):
    """Represents a Table DTO (Data Transfer Object)."""

    _name: str = field(metadata=config(field_name="name"))
    _columns: list[ColumnDTO] = field(metadata=config(field_name="columns"))
    _audit: AuditDTO = field(metadata=config(field_name="audit"))
    _comment: Optional[str] = field(
        default=None,
        metadata=config(field_name="comment", mm_field=Field(dump_default=None)),
    )
    _distribution: Optional[DistributionDTO] = field(
        default=None,
        metadata=config(field_name="distribution", mm_field=Field(dump_default=None)),
    )
    _sort_orders: Optional[list[SortOrderDTO]] = field(
        default=None,
        metadata=config(field_name="sortOrder", mm_field=Field(dump_default=None)),
    )
    _partitioning: Optional[list[Partitioning]] = field(
        default=None,
        metadata=config(field_name="partitioning", mm_field=Field(dump_default=None)),
    )
    _indexes: Optional[list[IndexDTO]] = field(
        default=None,
        metadata=config(field_name="indexes", mm_field=Field(dump_default=None)),
    )
    _properties: Optional[dict[str, str]] = field(
        default=None,
        metadata=config(field_name="properties", mm_field=Field(dump_default=None)),
    )

    def __post_init__(self):
        Precondition.check_argument(
            self._name is not None and self._name.strip() != "",
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
