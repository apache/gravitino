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

from typing import List, Optional

from gravitino.api.expressions.expression import Expression
from gravitino.api.expressions.named_reference import NamedReference
from gravitino.dto.rel.column_dto import ColumnDTO
from gravitino.dto.rel.partition_utils import PartitionUtils
from gravitino.dto.rel.partitioning.partitioning import Partitioning
from gravitino.dto.rel.partitions.range_partition_dto import RangePartitionDTO


class RangePartitioningDTO(Partitioning):
    """Data transfer object representing a range partitioning."""

    def __init__(
        self,
        field_name: List[str],
        assignments: Optional[List[RangePartitionDTO]] = None,
    ):
        self._field_name = field_name
        self._assignments = assignments if assignments is not None else []

    def field_name(self) -> List[str]:
        """Returns the name of the field to partition.

        Returns:
            List[str]: The name of the field to partition.
        """
        return self._field_name

    def assignments(self) -> List[RangePartitionDTO]:
        return self._assignments

    def strategy(self) -> Partitioning.Strategy:
        return self.Strategy.RANGE

    def validate(self, columns: List[ColumnDTO]) -> None:
        PartitionUtils.validate_field_existence(columns, self._field_name)

    def name(self) -> str:
        return self.strategy().name.lower()

    def arguments(self) -> List[Expression]:
        """Returns the arguments of the partitioning strategy.

        Returns:
            List[Expression]: The arguments of the partitioning strategy.
        """
        return [NamedReference.field(field_name=self._field_name)]
