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

from dataclasses_json import DataClassJsonMixin, config

from gravitino.api.function.function import Function
from gravitino.api.function.function_definition import FunctionDefinition
from gravitino.api.function.function_type import FunctionType
from gravitino.dto.audit_dto import AuditDTO
from gravitino.dto.function.function_definition_dto import FunctionDefinitionDTO


def _encode_function_type(func_type: FunctionType) -> str:
    """Encode FunctionType to string."""
    if func_type is None:
        return None
    return func_type.value


def _decode_function_type(func_type_str: str) -> FunctionType:
    """Decode string to FunctionType."""
    return FunctionType.from_string(func_type_str)


@dataclass
class FunctionDTO(Function, DataClassJsonMixin):
    """Represents a Function DTO (Data Transfer Object)."""

    _name: str = field(metadata=config(field_name="name"))
    _function_type: FunctionType = field(
        metadata=config(
            field_name="functionType",
            encoder=_encode_function_type,
            decoder=_decode_function_type,
        )
    )
    _deterministic: bool = field(metadata=config(field_name="deterministic"))
    _comment: Optional[str] = field(default=None, metadata=config(field_name="comment"))
    _definitions: Optional[List[FunctionDefinitionDTO]] = field(
        default=None, metadata=config(field_name="definitions")
    )
    _audit: Optional[AuditDTO] = field(
        default=None, metadata=config(field_name="audit")
    )

    def __post_init__(self):
        if self._function_type is None:
            raise ValueError("Function type cannot be null or empty")

    def name(self) -> str:
        """Returns the function name."""
        return self._name

    def function_type(self) -> FunctionType:
        """Returns the function type."""
        return self._function_type

    def deterministic(self) -> bool:
        """Returns whether the function is deterministic."""
        return self._deterministic

    def comment(self) -> Optional[str]:
        """Returns the optional comment of the function."""
        return self._comment

    def definitions(self) -> Optional[List[FunctionDefinition]]:
        """Returns the definitions of the function."""
        if self._definitions is None:
            return None
        return list(self._definitions)

    def audit_info(self) -> Optional[AuditDTO]:
        """Returns the audit information."""
        return self._audit

    def __eq__(self, other) -> bool:
        if not isinstance(other, FunctionDTO):
            return False
        return (
            self._name == other._name
            and self._function_type == other._function_type
            and self._deterministic == other._deterministic
            and self._comment == other._comment
            and self._definitions == other._definitions
        )

    def __hash__(self) -> int:
        return hash(
            (
                self._name,
                self._function_type,
                self._deterministic,
                self._comment,
            )
        )
