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


class BucketPartitioningDTO(Partitioning):
    """Data transfer object representing bucket partitioning."""

    def __init__(self, num_buckets: int, /, *field_names: List[str]):
        self._num_buckets = num_buckets
        self._field_names = [*field_names]

    def num_buckets(self) -> int:
        """Returns the number of buckets.

        Returns:
            int: The number of buckets.
        """
        return self._num_buckets

    def field_names(self) -> List[List[str]]:
        """Returns the field names.

        Returns:
            List[List[str]]: The field names.
        """
        return self._field_names

    def strategy(self) -> Partitioning.Strategy:
        return self.Strategy.BUCKET

    def validate(self, columns: List[ColumnDTO]) -> None:
        for field_name in self._field_names:
            PartitionUtils.validate_field_existence(columns, field_name)

    def name(self) -> str:
        return self.strategy().name.lower()

    def arguments(self) -> List[Expression]:
        """Gets the arguments of the partitioning strategy.

        Returns:
            List[Expression]: The arguments of the partitioning strategy.
        """
        return Transforms.bucket(self._num_buckets, *self._field_names).arguments()
