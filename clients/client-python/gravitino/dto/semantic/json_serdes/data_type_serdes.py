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

from typing import Optional

from gravitino.api.semantic.data_type import DataType
from gravitino.exceptions.base import IllegalArgumentException
from gravitino.utils.precondition import Precondition


class DataTypeSerdes:
    """Serdes for the Semantic Model logical data type vocabulary."""

    @staticmethod
    def serialize(value: Optional[DataType]) -> Optional[str]:
        """Encode a data type to its Ossie wire value."""
        return None if value is None else value.value

    @staticmethod
    def deserialize(value: Optional[str]) -> Optional[DataType]:
        """Decode a data type from its Ossie wire value."""
        if value is None:
            return None
        Precondition.check_argument(
            isinstance(value, str),
            f"DataType must be encoded as a string, but found {value}",
        )
        try:
            return DataType(value)
        except ValueError as error:
            raise IllegalArgumentException(
                f"Unknown Semantic Model data type: {value}"
            ) from error
