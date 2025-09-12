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

from typing import Any, Dict

from gravitino.api.expressions.sorts.null_ordering import NullOrdering
from gravitino.api.expressions.sorts.sort_direction import SortDirection
from gravitino.api.types.json_serdes import JsonSerializable
from gravitino.dto.rel.expressions.json_serdes._helper.serdes_utils import SerdesUtils
from gravitino.dto.rel.sort_order_dto import SortOrderDTO
from gravitino.utils.precondition import Precondition
from gravitino.utils.serdes import SerdesUtilsBase


class SortOrderSerdes(SerdesUtilsBase, JsonSerializable[SortOrderDTO]):
    """Custom JSON serializer/deserializer for SortOrderDTO objects."""

    @classmethod
    def serialize(cls, data_type: SortOrderDTO) -> dict[str, Any]:
        """
        Serialize the given data into a dictionary.

        Args:
            data_type (SortOrderDTO): The data to be serialized.

        Returns:
            dict[str, Any]: The serialized data.
        """

        return {
            cls.SORT_TERM: SerdesUtils.write_function_arg(data_type.sort_term()),
            cls.DIRECTION: str(data_type.direction()),
            cls.NULL_ORDERING: str(data_type.null_ordering()),
        }

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> SortOrderDTO:
        """
        Deserialize the given data into a `SortOrderDTO` instance.

        Args:
            data (dict[str, Any]): The data to be deserialized.

        Returns:
            SortOrderDTO: The deserialized `SortOrderDTO` instance.
        """

        Precondition.check_argument(
            isinstance(data, Dict) and len(data) > 0,
            f"Cannot parse sort order from invalid JSON: {data}",
        )
        Precondition.check_argument(
            cls.SORT_TERM in data,
            f"Cannot parse sort order from missing sort term: {data}",
        )
        sort_term_data = data[cls.SORT_TERM]
        Precondition.check_argument(
            sort_term_data is not None, "expression cannot be null"
        )
        direction_data = data.get(cls.DIRECTION)
        null_order_data = data.get(cls.NULL_ORDERING)
        sort_term = SerdesUtils.read_function_arg(data[cls.SORT_TERM])
        direction = (
            SortDirection.from_string(direction_data)
            if direction_data
            else SortDirection.ASCENDING
        )
        null_order = (
            NullOrdering(null_order_data)
            if null_order_data
            else direction.default_null_ordering()
        )

        return SortOrderDTO(sort_term, direction, null_order)
