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
from typing import Any, Dict, List, cast

from gravitino.dto.rel.expressions.json_serdes._helper.serdes_utils import (
    SerdesUtils as ExpressionsSerdesUtils,
)
from gravitino.dto.rel.expressions.literal_dto import LiteralDTO
from gravitino.dto.rel.partitions.identity_partition_dto import IdentityPartitionDTO
from gravitino.dto.rel.partitions.list_partition_dto import ListPartitionDTO
from gravitino.dto.rel.partitions.partition_dto import PartitionDTO
from gravitino.dto.rel.partitions.range_partition_dto import RangePartitionDTO
from gravitino.exceptions.base import IllegalArgumentException
from gravitino.utils.precondition import Precondition
from gravitino.utils.serdes import SerdesUtilsBase


class SerdesUtils(SerdesUtilsBase):
    @classmethod
    def write_partition(cls, value: PartitionDTO) -> Dict[str, Any]:
        result = {
            cls.PARTITION_TYPE: value.type().value,
            cls.PARTITION_NAME: value.name(),
        }
        dto_data = {}
        dto_type = value.type()

        if dto_type not in PartitionDTO.Type:
            raise IOError(f"Unknown partition type: {value.type()}")

        if dto_type is PartitionDTO.Type.IDENTITY:
            dto = cast(IdentityPartitionDTO, value)
            dto_data[cls.FIELD_NAMES] = dto.field_names()
            dto_data[cls.IDENTITY_PARTITION_VALUES] = [
                ExpressionsSerdesUtils.write_function_arg(arg=arg)
                for arg in dto.values()
            ]

        if dto_type is PartitionDTO.Type.LIST:
            dto = cast(ListPartitionDTO, value)
            dto_data[cls.LIST_PARTITION_LISTS] = [
                [ExpressionsSerdesUtils.write_function_arg(arg=arg) for arg in args]
                for args in dto.lists()
            ]

        if dto_type is PartitionDTO.Type.RANGE:
            dto = cast(RangePartitionDTO, value)
            dto_data[cls.RANGE_PARTITION_UPPER] = (
                ExpressionsSerdesUtils.write_function_arg(arg=dto.upper())
            )
            dto_data[cls.RANGE_PARTITION_LOWER] = (
                ExpressionsSerdesUtils.write_function_arg(arg=dto.lower())
            )

        dto_data[cls.PARTITION_PROPERTIES] = value.properties()
        result.update(dto_data)
        return result

    @classmethod
    def read_partition(cls, data: Dict[str, Any]) -> PartitionDTO:
        Precondition.check_argument(
            isinstance(data, Dict) and len(data) > 0,
            f"Partition must be a valid JSON object, but found: {data}",
        )
        Precondition.check_argument(
            data.get(cls.PARTITION_TYPE) is not None,
            f"Partition must have a type field, but found: {data}",
        )
        dto_type = None
        with suppress(ValueError):
            dto_type = PartitionDTO.Type(data[cls.PARTITION_TYPE])

        if dto_type is PartitionDTO.Type.IDENTITY:
            Precondition.check_argument(
                isinstance(data.get(cls.FIELD_NAMES), List),
                f"Identity partition must have array of fieldNames, but found: {data}",
            )
            Precondition.check_argument(
                isinstance(data.get(cls.IDENTITY_PARTITION_VALUES), List),
                f"Identity partition must have array of values, but found: {data}",
            )
            return IdentityPartitionDTO(
                name=data[cls.PARTITION_NAME],
                field_names=data[cls.FIELD_NAMES],
                values=[
                    cast(
                        LiteralDTO, ExpressionsSerdesUtils.read_function_arg(data=value)
                    )
                    for value in data[cls.IDENTITY_PARTITION_VALUES]
                ],
                properties=data.get(cls.PARTITION_PROPERTIES, {}),
            )

        if dto_type is PartitionDTO.Type.LIST:
            Precondition.check_argument(
                cls.PARTITION_NAME in data,
                f"List partition must have name, but found: {data}",
            )
            Precondition.check_argument(
                isinstance(data.get(cls.LIST_PARTITION_LISTS), List),
                f"List partition must have array of lists, but found: {data}",
            )
            return ListPartitionDTO(
                name=data[cls.PARTITION_NAME],
                lists=[
                    [
                        cast(
                            LiteralDTO,
                            ExpressionsSerdesUtils.read_function_arg(data=value),
                        )
                        for value in values
                    ]
                    for values in data[cls.LIST_PARTITION_LISTS]
                ],
                properties=data.get(cls.PARTITION_PROPERTIES, {}),
            )

        if dto_type is PartitionDTO.Type.RANGE:
            Precondition.check_argument(
                cls.PARTITION_NAME in data,
                f"Range partition must have name, but found: {data}",
            )
            Precondition.check_argument(
                cls.RANGE_PARTITION_UPPER in data,
                f"Range partition must have upper, but found: {data}",
            )
            Precondition.check_argument(
                cls.RANGE_PARTITION_LOWER in data,
                f"Range partition must have lower, but found: {data}",
            )
            upper = cast(
                LiteralDTO,
                ExpressionsSerdesUtils.read_function_arg(
                    data[cls.RANGE_PARTITION_UPPER]
                ),
            )
            lower = cast(
                LiteralDTO,
                ExpressionsSerdesUtils.read_function_arg(
                    data[cls.RANGE_PARTITION_LOWER]
                ),
            )
            return RangePartitionDTO(
                name=data[cls.PARTITION_NAME],
                upper=upper,
                lower=lower,
                properties=data.get(cls.PARTITION_PROPERTIES, {}),
            )

        raise IllegalArgumentException(
            f"Unknown partition type: {data[cls.PARTITION_TYPE]}"
        )
