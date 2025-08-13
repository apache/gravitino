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
from gravitino.dto.rel.partitions.list_partition_dto import ListPartitionDTO


class ListPartitioningDTO(Partitioning):
    """Data transfer object representing a list partitioning."""

    def __init__(
        self,
        field_names: List[List[str]],
        assignments: Optional[List[ListPartitionDTO]] = None,
    ):
        self._field_names = field_names
        self._assignments = assignments if assignments is not None else []

    def field_names(self) -> List[List[str]]:
        """Returns he names of the fields to partition.

        Returns:
            List[List[str]]: The names of the fields to partition.
        """
        return self._field_names

    def assignments(self) -> List[ListPartitionDTO]:
        return self._assignments

    def strategy(self) -> Partitioning.Strategy:
        return self.Strategy.LIST

    def validate(self, columns: List[ColumnDTO]) -> None:
        for field_name in self._field_names:
            PartitionUtils.validate_field_existence(columns, field_name)

    def name(self) -> str:
        return self.strategy().name.lower()

    def arguments(self) -> List[Expression]:
        """Returns the arguments of the partitioning strategy.

        Returns:
            List[Expression]: The arguments of the partitioning strategy.
        """
        return [
            NamedReference.field(field_name=field_name)
            for field_name in self._field_names
        ]
