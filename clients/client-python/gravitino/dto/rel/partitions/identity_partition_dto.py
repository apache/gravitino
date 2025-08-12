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


from typing import Dict, List

from gravitino.api.expressions.partitions.identity_partition import IdentityPartition
from gravitino.dto.rel.expressions.literal_dto import LiteralDTO
from gravitino.dto.rel.partitions.partition_dto import PartitionDTO


class IdentityPartitionDTO(PartitionDTO, IdentityPartition):
    """Represents an Identity Partition Data Transfer Object (DTO) that implements the IdentityPartition interface."""

    def __init__(
        self,
        name: str,
        values: List[LiteralDTO],
        field_names: List[List[str]],
        properties: Dict[str, str],
    ):
        self._name = name
        self._values = values
        self._field_names = field_names
        self._properties = properties

    def name(self) -> str:
        return self._name

    def values(self) -> List[LiteralDTO]:
        return self._values

    def field_names(self) -> List[List[str]]:
        return self._field_names

    def properties(self) -> Dict[str, str]:
        return self._properties

    def type(self) -> PartitionDTO.Type:
        return self.Type.IDENTITY

    def __eq__(self, value: object) -> bool:
        if not isinstance(value, IdentityPartitionDTO):
            return False
        return (
            self is value
            or self._name == value.name()
            and self._values == value.values()
            and self._field_names == value.field_names()
            and self._properties == value.properties()
        )

    def __hash__(self) -> int:
        return hash(
            (
                self._name,
                tuple(self._values),
                tuple(tuple(field) for field in self._field_names),
                tuple(self._properties.items()),
            )
        )
