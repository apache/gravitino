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


from typing import Dict

from gravitino.api.expressions.partitions.range_partition import RangePartition
from gravitino.dto.rel.expressions.literal_dto import LiteralDTO
from gravitino.dto.rel.partitions.partition_dto import PartitionDTO


class RangePartitionDTO(PartitionDTO, RangePartition):
    """Data transfer object representing a range partition."""

    def __init__(
        self,
        name: str,
        properties: Dict[str, str],
        upper: LiteralDTO,
        lower: LiteralDTO,
    ):
        self._name = name
        self._properties = properties
        self._upper = upper
        self._lower = lower

    def name(self) -> str:
        return self._name

    def properties(self) -> Dict[str, str]:
        return self._properties

    def upper(self) -> LiteralDTO:
        return self._upper

    def lower(self) -> LiteralDTO:
        return self._lower

    def type(self) -> PartitionDTO.Type:
        return self.Type.RANGE

    def __eq__(self, value: object) -> bool:
        if not isinstance(value, RangePartitionDTO):
            return False

        return (
            self is value
            or self._name == value.name()
            and self._properties == value.properties()
            and self._upper == value.upper()
            and self._lower == value.lower()
        )

    def __hash__(self) -> int:
        return hash(
            (self._name, tuple(self._properties.items()), self._upper, self._lower)
        )
