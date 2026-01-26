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

from dataclasses import dataclass, field
from typing import List, Optional

from dataclasses_json import config

from gravitino.api.function.function_type import FunctionType
from gravitino.api.rel.types.json_serdes.type_serdes import TypeSerdes
from gravitino.api.rel.types.type import Type
from gravitino.dto.function.function_column_dto import FunctionColumnDTO
from gravitino.dto.function.function_definition_dto import FunctionDefinitionDTO
from gravitino.exceptions.base import IllegalArgumentException
from gravitino.rest.rest_message import RESTRequest


def _encode_function_type(func_type: FunctionType) -> str:
    """Encode FunctionType to string."""
    if func_type is None:
        return None
    return func_type.value


def _decode_function_type(func_type_str: str) -> FunctionType:
    """Decode string to FunctionType."""
    if func_type_str is None:
        return None
    return FunctionType.from_string(func_type_str)


@dataclass
class FunctionRegisterRequest(RESTRequest):
    """Represents a request to register a function."""

    _name: str = field(metadata=config(field_name="name"))
    _function_type: FunctionType = field(
        metadata=config(
            field_name="functionType",
            encoder=_encode_function_type,
            decoder=_decode_function_type,
        )
    )
    _deterministic: bool = field(metadata=config(field_name="deterministic"))
    _definitions: List[FunctionDefinitionDTO] = field(
        metadata=config(field_name="definitions")
    )
    _comment: Optional[str] = field(default=None, metadata=config(field_name="comment"))
    _return_type: Optional[Type] = field(
        default=None,
        metadata=config(
            field_name="returnType",
            encoder=TypeSerdes.serialize,
            decoder=TypeSerdes.deserialize,
        ),
    )
    _return_columns: Optional[List[FunctionColumnDTO]] = field(
        default=None, metadata=config(field_name="returnColumns")
    )

    def __init__(
        self,
        name: str,
        function_type: FunctionType,
        deterministic: bool,
        definitions: List[FunctionDefinitionDTO],
        comment: Optional[str] = None,
        return_type: Optional[Type] = None,
        return_columns: Optional[List[FunctionColumnDTO]] = None,
    ):
        self._name = name
        self._function_type = function_type
        self._deterministic = deterministic
        self._definitions = definitions
        self._comment = comment
        self._return_type = return_type
        self._return_columns = return_columns

    def validate(self):
        """Validates the request.

        Raises:
            IllegalArgumentException: If the request is invalid.
        """
        if not self._name:
            raise IllegalArgumentException(
                "'name' field is required and cannot be empty"
            )
        if self._function_type is None:
            raise IllegalArgumentException("'functionType' field is required")
        if not self._definitions:
            raise IllegalArgumentException(
                "'definitions' field is required and cannot be empty"
            )

        if self._function_type == FunctionType.TABLE:
            if not self._return_columns:
                raise IllegalArgumentException(
                    "'returnColumns' is required for TABLE function type"
                )
        else:
            if self._return_type is None:
                raise IllegalArgumentException(
                    "'returnType' is required for SCALAR or AGGREGATE function type"
                )
