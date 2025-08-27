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

from __future__ import annotations

from typing import List

from gravitino.api.expressions.named_reference import NamedReference
from gravitino.dto.rel.expressions.function_arg import FunctionArg


class FieldReferenceDTO(NamedReference, FunctionArg):
    """Data transfer object representing a field reference."""

    def __init__(self, field_name: List[str]):
        self._field_name = field_name

    def field_name(self) -> List[str]:
        return self._field_name

    def arg_type(self) -> FunctionArg.ArgType:
        return self.ArgType.FIELD

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, FieldReferenceDTO):
            return False
        return self._field_name == other.field_name()

    def __hash__(self) -> int:
        return hash((self.arg_type(), tuple(self._field_name)))

    @staticmethod
    def builder() -> Builder:
        """The builder for creating a new instance of `FieldReferenceDTO`.

        Returns:
            Builder: The builder for creating a new instance of `FieldReferenceDTO`.
        """
        return FieldReferenceDTO.Builder()

    class Builder:
        """Builder for `FieldRererenceDTO`"""

        def __init__(self):
            self._field_name = None

        def with_field_name(self, field_name: List[str]) -> FieldReferenceDTO.Builder:
            """Set the field name for the field reference.

            Args:
                field_name (List[str]): The field name.

            Returns:
                FieldReferenceDTO.Builder: The builder.
            """

            self._field_name = field_name
            return self

        def with_column_name(self, column_name: str) -> FieldReferenceDTO.Builder:
            """Set the column name for the field reference.

            Args:
                column_name (str): The column name.

            Returns:
                FieldReferenceDTO.Builder: The builder.
            """

            self._field_name = [column_name]
            return self

        def build(self) -> FieldReferenceDTO:
            """Build the field reference.

            Returns:
                FieldReferenceDTO: The field reference.
            """

            return FieldReferenceDTO(field_name=self._field_name)
