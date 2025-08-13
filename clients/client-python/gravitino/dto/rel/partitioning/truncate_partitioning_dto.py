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

from typing import List

from gravitino.api.expressions.expression import Expression
from gravitino.api.expressions.transforms.transforms import Transforms
from gravitino.dto.rel.column_dto import ColumnDTO
from gravitino.dto.rel.partition_utils import PartitionUtils
from gravitino.dto.rel.partitioning.partitioning import Partitioning


class TruncatePartitioningDTO(Partitioning):
    """Represents the truncate partitioning.

    Attributes:
        _width (int): The width of the truncate partitioning.
        _field_name (List[str]): The name of the field to partition.
    """

    def __init__(self, width: int, field_name: List[str]):
        self._field_name = field_name
        self._width = width

    def field_name(self) -> List[str]:
        """Returns the name of the field to partition.

        Returns:
            List[str]: The name of the field to partition.
        """
        return self._field_name

    def width(self) -> int:
        """Returns the width of the partitioning.

        Returns:
            int: The width of the partitioning.
        """
        return self._width

    def strategy(self) -> Partitioning.Strategy:
        return self.Strategy.TRUNCATE

    def validate(self, columns: List[ColumnDTO]) -> None:
        PartitionUtils.validate_field_existence(columns, self._field_name)

    def name(self) -> str:
        return self.strategy().name.lower()

    def arguments(self) -> List[Expression]:
        """Returns the arguments of the partitioning strategy.

        Returns:
            List[Expression]: The arguments of the partitioning strategy.
        """
        return Transforms.truncate(
            width=self._width, field_name=self._field_name
        ).arguments()
