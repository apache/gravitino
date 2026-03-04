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

from enum import Enum


class FunctionType(Enum):
    """Function type supported by Gravitino."""

    SCALAR = "scalar"
    """Scalar function that returns a single value per row."""

    AGGREGATE = "aggregate"
    """Aggregate function that combines multiple rows into a single value."""

    TABLE = "table"
    """Table-valued function that returns a table of rows."""

    @classmethod
    def from_string(cls, type_str: str) -> "FunctionType":
        """Parse the function type from a string value.

        Args:
            type_str: The string to parse.

        Returns:
            The parsed FunctionType.

        Raises:
            ValueError: If the value cannot be parsed.
        """
        if not type_str:
            raise ValueError("Function type cannot be null or empty")

        type_lower = type_str.lower()
        if type_lower == "agg":
            return cls.AGGREGATE

        for func_type in cls:
            if func_type.value == type_lower:
                return func_type

        raise ValueError(f"Invalid function type: {type_str}")

    def type_name(self) -> str:
        """Returns the canonical string representation used by APIs."""
        return self.value
