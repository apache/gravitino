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
from typing import Any, Dict, List, Optional

from dataclasses_json import DataClassJsonMixin, config

from gravitino.api.function.function_column import FunctionColumn
from gravitino.api.function.function_definition import (
    FunctionDefinition,
    FunctionDefinitions,
)
from gravitino.api.function.function_impl import FunctionImpl
from gravitino.api.function.function_param import FunctionParam
from gravitino.api.rel.types.json_serdes.type_serdes import TypeSerdes
from gravitino.api.rel.types.type import Type
from gravitino.dto.function.function_column_dto import FunctionColumnDTO
from gravitino.dto.function.function_impl_dto import (
    FunctionImplDTO,
    JavaImplDTO,
    PythonImplDTO,
    SQLImplDTO,
    function_impl_dto_from_function_impl,
)
from gravitino.dto.function.function_param_dto import FunctionParamDTO


def _decode_impl(impl_dict: Dict[str, Any]) -> FunctionImplDTO:
    """Decode a function implementation DTO from a dictionary."""
    if impl_dict is None:
        return None

    language = impl_dict.get("language")
    if language == "SQL":
        return SQLImplDTO.from_dict(impl_dict)
    if language == "JAVA":
        return JavaImplDTO.from_dict(impl_dict)
    if language == "PYTHON":
        return PythonImplDTO.from_dict(impl_dict)

    raise ValueError(f"Unsupported implementation language: {language}")


def _decode_impls(impls_list: List[Dict[str, Any]]) -> List[FunctionImplDTO]:
    """Decode a list of function implementation DTOs from a list of dictionaries."""
    if impls_list is None:
        return []
    return [_decode_impl(impl) for impl in impls_list]


def _encode_impl(impl: FunctionImplDTO) -> Dict[str, Any]:
    """Encode a function implementation DTO to a dictionary."""
    if impl is None:
        return None
    result = impl.to_dict()
    result["language"] = impl.language().name
    return result


def _encode_impls(impls: List[FunctionImplDTO]) -> List[Dict[str, Any]]:
    """Encode a list of function implementation DTOs to a list of dictionaries."""
    if impls is None:
        return []
    return [_encode_impl(impl) for impl in impls]


@dataclass
class FunctionDefinitionDTO(FunctionDefinition, DataClassJsonMixin):
    """DTO for function definition."""

    _parameters: Optional[List[FunctionParamDTO]] = field(
        default=None, metadata=config(field_name="parameters")
    )
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
    _impls: Optional[List[FunctionImplDTO]] = field(
        default=None,
        metadata=config(
            field_name="impls",
            encoder=_encode_impls,
            decoder=_decode_impls,
        ),
    )

    def parameters(self) -> List[FunctionParam]:
        """Returns the parameters for this definition."""
        return list(self._parameters) if self._parameters else []

    def return_type(self) -> Optional[Type]:
        """Returns the return type for scalar or aggregate functions."""
        return self._return_type

    def return_columns(self) -> List[FunctionColumn]:
        """Returns the output columns for a table-valued function."""
        if self._return_columns is None:
            return FunctionDefinition.EMPTY_COLUMNS
        return [col.to_function_column() for col in self._return_columns]

    def impls(self) -> List[FunctionImpl]:
        """Returns the implementations associated with this definition."""
        if self._impls is None:
            return []
        return [impl.to_function_impl() for impl in self._impls]

    def to_function_definition(self) -> FunctionDefinition:
        """Convert this DTO to a FunctionDefinition instance."""
        params = (
            [p.to_function_param() for p in self._parameters]
            if self._parameters
            else []
        )
        impls = [impl.to_function_impl() for impl in self._impls] if self._impls else []

        if self._return_type is not None:
            return FunctionDefinitions.of(params, self._return_type, impls)
        if self._return_columns and len(self._return_columns) > 0:
            cols = [col.to_function_column() for col in self._return_columns]
            return FunctionDefinitions.of_table(params, cols, impls)
        # Fallback for backward compatibility
        return FunctionDefinitions.SimpleFunctionDefinition(params, None, None, impls)

    @classmethod
    def from_function_definition(
        cls, definition: FunctionDefinition
    ) -> "FunctionDefinitionDTO":
        """Create a FunctionDefinitionDTO from a FunctionDefinition instance."""
        param_dtos = (
            [
                (
                    p
                    if isinstance(p, FunctionParamDTO)
                    else FunctionParamDTO.from_function_param(p)
                )
                for p in definition.parameters()
            ]
            if definition.parameters()
            else []
        )

        return_column_dtos = None
        if definition.return_columns() and len(definition.return_columns()) > 0:
            return_column_dtos = [
                FunctionColumnDTO.from_function_column(col)
                for col in definition.return_columns()
            ]

        impl_dtos = (
            [function_impl_dto_from_function_impl(impl) for impl in definition.impls()]
            if definition.impls()
            else []
        )

        return cls(
            _parameters=param_dtos,
            _return_type=definition.return_type(),
            _return_columns=return_column_dtos,
            _impls=impl_dtos,
        )

    def __eq__(self, other) -> bool:
        if not isinstance(other, FunctionDefinitionDTO):
            return False
        return (
            self._parameters == other._parameters
            and self._return_type == other._return_type
            and self._return_columns == other._return_columns
            and self._impls == other._impls
        )

    def __hash__(self) -> int:
        return hash(
            (
                tuple(self._parameters) if self._parameters else None,
                self._return_type,
                tuple(self._return_columns) if self._return_columns else None,
                len(self._impls) if self._impls is not None else None,
            )
        )
