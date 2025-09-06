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


from typing import Any

from gravitino.api.expressions.distributions.strategy import Strategy
from gravitino.api.types.json_serdes.base import JsonSerializable
from gravitino.dto.rel.distribution_dto import DistributionDTO
from gravitino.dto.rel.expressions.json_serdes._helper.serdes_utils import SerdesUtils
from gravitino.utils.precondition import Precondition
from gravitino.utils.serdes import SerdesUtilsBase


class DistributionSerDes(SerdesUtilsBase, JsonSerializable[DistributionDTO]):
    """Custom JSON deserializer for DistributionDTO objects."""

    @classmethod
    def serialize(cls, data_type: DistributionDTO) -> dict[str, Any]:
        return {
            cls.STRATEGY: data_type.strategy().name.lower(),
            cls.NUMBER: data_type.number(),
            cls.FUNCTION_ARGS: [
                SerdesUtils.write_function_arg(arg) for arg in data_type.args()
            ],
        }

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> DistributionDTO:
        Precondition.check_argument(
            isinstance(data, dict) and len(data) > 0,
            f"Cannot parse distribution from invalid JSON: {data}",
        )
        strategy_data = data.get(cls.STRATEGY, Strategy.HASH.value)
        return DistributionDTO(
            strategy=Strategy(strategy_data.upper()),
            number=data[cls.NUMBER],
            args=[
                SerdesUtils.read_function_arg(arg) for arg in data[cls.FUNCTION_ARGS]
            ],
        )
