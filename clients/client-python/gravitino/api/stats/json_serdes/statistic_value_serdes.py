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

from dataclasses_json.core import Json

from gravitino.api.rel.types.json_serdes.base import JsonSerializable
from gravitino.api.rel.types.type import Name
from gravitino.api.stats.statistic_value import StatisticValue
from gravitino.api.stats.statistic_values import StatisticValues
from gravitino.utils.precondition import Precondition


class StatisticValueSerdes(JsonSerializable[StatisticValue[Any]]):
    """Customized JSON Serializer and Deserializer for StatisticValue."""

    @classmethod
    def serialize(cls, value: StatisticValue[Any]) -> Json:
        """Serialize the given StatisticValue.

        Args:
            value (StatisticValue): The StatisticValue to be serialized.

        Returns:
            Json: The serialized data corresponding to the given Gravitino Type.
        """

        match value.data_type().name():
            case Name.BOOLEAN | Name.STRING | Name.DOUBLE | Name.LONG:
                return value.value()
            case Name.LIST:
                return [cls.serialize(item) for item in value.value()]
            case Name.STRUCT:
                return {k: cls.serialize(v) for k, v in value.value().items()}
            case _:
                raise ValueError(
                    f"Unsupported statistic value type: {value.data_type()}"
                )

    @classmethod
    def deserialize(cls, data: Json) -> StatisticValue[Any]:
        """Deserialize the given data to a StatisticValue.

        Args:
            data (Json): The data to be deserialized.

        Returns:
            StatisticValue[Any]: The deserialized StatisticValue.
        """

        return cls._get_statistic_value(data)

    @classmethod
    def _get_statistic_value(cls, data: Json) -> StatisticValue[Any]:
        """Get the StatisticValue from the given data.

        Args:
            data (Json): The data to get the StatisticValue from.

        Returns:
            StatisticValue: The StatisticValue corresponding to the given data.
        """

        Precondition.check_argument(
            data is not None, f"Cannot parse statistic value from invalid JSON: {data}"
        )

        match data:
            case bool():
                return StatisticValues.boolean_value(data)
            case int():
                return StatisticValues.long_value(data)
            case float():
                return StatisticValues.double_value(data)
            case str():
                return StatisticValues.string_value(data)
            case list():
                return StatisticValues.list_value(
                    [cls._get_statistic_value(item) for item in data]
                )
            case dict():
                return StatisticValues.object_value(
                    {
                        key: cls._get_statistic_value(value)
                        for key, value in data.items()
                    }
                )
            case _:
                raise ValueError(
                    f"Unsupported data type for statistic value: {type(data)}"
                )
