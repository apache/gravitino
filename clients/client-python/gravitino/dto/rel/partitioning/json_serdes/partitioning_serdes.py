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
from gravitino.dto.rel.expressions.json_serdes._helper.serdes_utils import (
    SerdesUtils as ExpressionSerdesUtils,
)
from gravitino.dto.rel.partitioning.bucket_partitioning_dto import (
    BucketPartitioningDTO,
)
from gravitino.dto.rel.partitioning.day_partitioning_dto import DayPartitioningDTO
from gravitino.dto.rel.partitioning.function_partitioning_dto import (
    FunctionPartitioningDTO,
)
from gravitino.dto.rel.partitioning.hour_partitioning_dto import HourPartitioningDTO
from gravitino.dto.rel.partitioning.identity_partitioning_dto import (
    IdentityPartitioningDTO,
)
from gravitino.dto.rel.partitioning.list_partitioning_dto import ListPartitioningDTO
from gravitino.dto.rel.partitioning.month_partitioning_dto import MonthPartitioningDTO
from gravitino.dto.rel.partitioning.partitioning import (
    Partitioning,
    SingleFieldPartitioning,
)
from gravitino.dto.rel.partitioning.range_partitioning_dto import RangePartitioningDTO
from gravitino.dto.rel.partitioning.truncate_partitioning_dto import (
    TruncatePartitioningDTO,
)
from gravitino.dto.rel.partitioning.year_partitioning_dto import YearPartitioningDTO
from gravitino.dto.rel.partitions.json_serdes._helper.serdes_utils import (
    SerdesUtils as PartitionSerdesUtils,
)
from gravitino.dto.rel.partitions.list_partition_dto import ListPartitionDTO
from gravitino.dto.rel.partitions.range_partition_dto import RangePartitionDTO
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
            dto = cast(SingleFieldPartitioning, data_type)
            return {**result, cls.FIELD_NAME: dto.field_name()}
        if strategy is Partitioning.Strategy.BUCKET:
            dto = cast(BucketPartitioningDTO, data_type)
            return {
                **result,
                cls.NUM_BUCKETS: dto.num_buckets(),
                cls.FIELD_NAMES: dto.field_names(),
            }
        if strategy is Partitioning.Strategy.TRUNCATE:
            dto = cast(TruncatePartitioningDTO, data_type)
            return {
                **result,
                cls.WIDTH: dto.width(),
                cls.FIELD_NAME: dto.field_name(),
            }
        if strategy is Partitioning.Strategy.LIST:
            dto = cast(ListPartitioningDTO, data_type)
            return {
                **result,
                cls.FIELD_NAMES: dto.field_names(),
                cls.ASSIGNMENTS_NAME: [
                    PartitionSerdesUtils.write_partition(list_partition_dto)
                    for list_partition_dto in dto.assignments()
                ],
            }
        if strategy is Partitioning.Strategy.RANGE:
            dto = cast(RangePartitioningDTO, data_type)
            return {
                **result,
                cls.FIELD_NAME: dto.field_name(),
                cls.ASSIGNMENTS_NAME: [
                    PartitionSerdesUtils.write_partition(range_partition_dto)
                    for range_partition_dto in dto.assignments()
                ],
            }
        if strategy is Partitioning.Strategy.FUNCTION:
            dto = cast(FunctionPartitioningDTO, data_type)
            return {
                **result,
                cls.FUNCTION_NAME: dto.function_name(),
                cls.FUNCTION_ARGS: [
                    ExpressionSerdesUtils.write_function_arg(function_arg)
                    for function_arg in dto.args()
                ],
            }

        raise IOError(f"Unknown partitioning strategy: {strategy}")

    @classmethod
    def deserialize(cls, data: Dict[str, Any]) -> Partitioning:
        """Deserialize the given JSON data to the corresponding Partitioning object.

        Args:
            data (Dict[str, Any]): The JSON data object to be deserialized.

        Returns:
            Partitioning: The deserialized result.

        Raises:
            IllegalArgumentException: If there's illegal arguments in the given JSON data.
        """

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
        if strategy is Partitioning.Strategy.BUCKET:
            return BucketPartitioningDTO(
                int(data[cls.NUM_BUCKETS]),
                *data.get(cls.FIELD_NAMES, []),
            )
        if strategy is Partitioning.Strategy.TRUNCATE:
            return TruncatePartitioningDTO(
                int(data[cls.WIDTH]),
                data.get(cls.FIELD_NAME, []),
            )
        if strategy is Partitioning.Strategy.LIST:
            field_names = data[cls.FIELD_NAMES]
            assignments_data = data.get(cls.ASSIGNMENTS_NAME, [])
            Precondition.check_argument(
                isinstance(assignments_data, list),
                f"Cannot parse list partitioning from non-array assignments: {assignments_data}",
            )
            assignments = []
            for assignment in assignments_data:
                partition_dto = PartitionSerdesUtils.read_partition(assignment)
                Precondition.check_argument(
                    isinstance(partition_dto, ListPartitionDTO),
                    f"Cannot parse list partitioning from non-list assignment: {assignment}",
                )
                assignments.append(partition_dto)
            return ListPartitioningDTO(field_names, assignments)
        if strategy is Partitioning.Strategy.RANGE:
            fields = data[cls.FIELD_NAME]
            assignments_data = data.get(cls.ASSIGNMENTS_NAME, [])
            Precondition.check_argument(
                isinstance(assignments_data, list),
                f"Cannot parse range partitioning from non-array assignments: {assignments_data}",
            )
            assignments = []
            for assignment in assignments_data:
                partition_dto = PartitionSerdesUtils.read_partition(assignment)
                Precondition.check_argument(
                    isinstance(partition_dto, RangePartitionDTO),
                    f"Cannot parse range partitioning from non-range assignment: {assignment}",
                )
                assignments.append(partition_dto)
            return RangePartitioningDTO(fields, assignments)
        if strategy is Partitioning.Strategy.FUNCTION:
            function_name = data[cls.FUNCTION_NAME]
            Precondition.check_argument(
                data.get(cls.FUNCTION_ARGS) is not None,
                f"Cannot parse function partitioning from missing function args: {data}",
            )
            args = [
                ExpressionSerdesUtils.read_function_arg(arg)
                for arg in data[cls.FUNCTION_ARGS]
            ]
            return FunctionPartitioningDTO(function_name, *args)

        raise IOError(f"Unknown partitioning strategy: {data[cls.STRATEGY]}")
