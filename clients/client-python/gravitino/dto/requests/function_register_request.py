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
from gravitino.dto.function.function_definition_dto import FunctionDefinitionDTO
from gravitino.dto.function.function_dto import (
    _decode_function_type,
    _encode_function_type,
)
from gravitino.exceptions.base import IllegalArgumentException
from gravitino.rest.rest_message import RESTRequest


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

    def __init__(
        self,
        name: str,
        function_type: FunctionType,
        deterministic: bool,
        definitions: List[FunctionDefinitionDTO],
        comment: Optional[str] = None,
    ):
        self._name = name
        self._function_type = function_type
        self._deterministic = deterministic
        self._definitions = definitions
        self._comment = comment

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

        # Validate each definition has appropriate return type/columns based on function type
        for definition in self._definitions:
            if self._function_type == FunctionType.TABLE:
                if (
                    not definition.return_columns()
                    or len(definition.return_columns()) == 0
                ):
                    raise IllegalArgumentException(
                        "'returnColumns' is required in each definition for TABLE function type"
                    )
            else:
                if definition.return_type() is None:
                    raise IllegalArgumentException(
                        "'returnType' is required in each definition for SCALAR or AGGREGATE function type"
                    )
