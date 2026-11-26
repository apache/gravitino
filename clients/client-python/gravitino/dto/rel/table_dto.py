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
from gravitino.api.rel.supports_partition_specs import SupportsPartitionSpecs
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
from gravitino.exceptions.base import UnsupportedOperationException
from gravitino.utils.precondition import Precondition


def _encode_partition_specs(
    specs: Optional[dict[int, list[Partitioning]]],
) -> Optional[dict[str, list]]:
    if specs is None:
        return None
    return {
        str(spec_id): [PartitioningSerdes.serialize(item) for item in transforms]
        for spec_id, transforms in specs.items()
    }


def _decode_partition_specs(values) -> Optional[dict[int, list[Partitioning]]]:
    if values is None:
        return None
    return {
        int(spec_id): [PartitioningSerdes.deserialize(item) for item in transforms]
        for spec_id, transforms in values.items()
    }


def _partitioning_equal(left: list[Partitioning], right: list[Partitioning]) -> bool:
    if len(left) != len(right):
        return False
    return all(
        PartitioningSerdes.serialize(lhs) == PartitioningSerdes.serialize(rhs)
        for lhs, rhs in zip(left, right)
    )


@dataclass
class TableDTO(
    Table, SupportsPartitionSpecs, DataClassJsonMixin
):  # pylint: disable=R0902
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
    _default_partition_spec_id: Optional[int] = field(
        default=None,
        metadata=config(
            field_name="defaultPartitionSpecId",
            exclude=lambda value: value is None,
        ),
    )
    _partition_specs: Optional[dict[int, list[Partitioning]]] = field(
        default=None,
        metadata=config(
            field_name="partitionSpecs",
            encoder=_encode_partition_specs,
            decoder=_decode_partition_specs,
            exclude=lambda value: value is None,
        ),
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
        self._validate_partition_spec_fields()

    def _validate_partition_spec_fields(self) -> None:
        has_default = self._default_partition_spec_id is not None
        has_specs = self._partition_specs is not None
        Precondition.check_argument(
            has_default == has_specs,
            "defaultPartitionSpecId and partitionSpecs must both be set or both be null",
        )
        if not has_specs:
            return
        Precondition.check_argument(
            self._default_partition_spec_id in self._partition_specs,
            f"defaultPartitionSpecId {self._default_partition_spec_id} is missing "
            f"from partitionSpecs {list(self._partition_specs.keys())}",
        )
        current = self._partitioning or []
        expected = self._partition_specs[self._default_partition_spec_id] or []
        Precondition.check_argument(
            _partitioning_equal(current, expected),
            f"partitioning must match partitionSpecs[{self._default_partition_spec_id}]",
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

    def supports_partition_specs(self) -> SupportsPartitionSpecs:
        if self._partition_specs is None or self._default_partition_spec_id is None:
            raise UnsupportedOperationException(
                "Table does not support partition specs."
            )
        self._validate_partition_spec_fields()
        return self

    def default_partition_spec_id(self) -> int:
        Precondition.check_argument(
            self._default_partition_spec_id is not None,
            "Table does not carry a default partition spec id",
        )
        return self._default_partition_spec_id

    def partition_specs(self) -> dict[int, list[Transform]]:
        Precondition.check_argument(
            self._partition_specs is not None,
            "Table does not carry partition specs",
        )
        return {
            spec_id: list(transforms)
            for spec_id, transforms in self._partition_specs.items()
        }
