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

from contextlib import suppress
from types import MappingProxyType
from typing import Any, Dict, Final, cast

from gravitino.api.types.json_serdes.base import JsonSerializable
from gravitino.dto.rel.partitioning.day_partitioning_dto import DayPartitioningDTO
from gravitino.dto.rel.partitioning.hour_partitioning_dto import HourPartitioningDTO
from gravitino.dto.rel.partitioning.identity_partitioning_dto import (
    IdentityPartitioningDTO,
)
from gravitino.dto.rel.partitioning.month_partitioning_dto import MonthPartitioningDTO
from gravitino.dto.rel.partitioning.partitioning import (
    Partitioning,
    SingleFieldPartitioning,
)
from gravitino.dto.rel.partitioning.year_partitioning_dto import YearPartitioningDTO
from gravitino.utils.precondition import Precondition
from gravitino.utils.serdes import SerdesUtilsBase


class PartitioningSerdes(SerdesUtilsBase, JsonSerializable[Partitioning]):
    """Custom JSON serializer/deserializer for PartitionDTO objects."""

    _SINGLE_FIELD_PARTITIONING: Final[MappingProxyType] = MappingProxyType(
        {
            Partitioning.Strategy.IDENTITY: IdentityPartitioningDTO,
            Partitioning.Strategy.YEAR: YearPartitioningDTO,
            Partitioning.Strategy.MONTH: MonthPartitioningDTO,
            Partitioning.Strategy.DAY: DayPartitioningDTO,
            Partitioning.Strategy.HOUR: HourPartitioningDTO,
        }
    )

    @classmethod
    def serialize(cls, data_type: Partitioning) -> Dict[str, Any]:
        """Serialize the given PartitionDTO object.

        Args:
            data_type (Partitioning): The PartitionDTO objects.

        Returns:
            Dict[str, Any]: The serialized result.

        Raises:
            IOError: If partitioning strategy is unknown.
        """

        strategy = data_type.strategy()
        result = {cls.STRATEGY: strategy.name.lower()}

        if strategy in cls._SINGLE_FIELD_PARTITIONING:
            partitioning = cast(SingleFieldPartitioning, data_type)
            return {**result, cls.FIELD_NAME: partitioning.field_name()}

        raise IOError(f"Unknown partitioning strategy: {strategy}")

    @classmethod
    def deserialize(cls, data: Dict[str, Any]) -> Partitioning:
        Precondition.check_argument(
            isinstance(data, dict) and len(data) > 0,
            f"Cannot parse partitioning from invalid JSON: {data}",
        )
        Precondition.check_argument(
            cls.STRATEGY in data,
            f"Cannot parse partitioning from missing strategy: {data}",
        )
        strategy = None
        with suppress(ValueError):
            strategy = Partitioning.Strategy(data[cls.STRATEGY].lower())

        if strategy in cls._SINGLE_FIELD_PARTITIONING:
            return cls._SINGLE_FIELD_PARTITIONING[strategy](
                *data.get(cls.FIELD_NAME, [])
            )

        raise IOError(f"Unknown partitioning strategy: {data[cls.STRATEGY]}")
