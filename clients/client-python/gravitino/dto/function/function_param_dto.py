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
from typing import Any, Dict, Optional

from dataclasses_json import DataClassJsonMixin, config

from gravitino.api.function.function_param import FunctionParam, FunctionParams
from gravitino.api.rel.expressions.expression import Expression
from gravitino.api.rel.types.json_serdes.type_serdes import TypeSerdes
from gravitino.api.rel.types.type import Type
from gravitino.dto.rel.expressions.function_arg import FunctionArg
from gravitino.dto.rel.expressions.json_serdes._helper.serdes_utils import (
    SerdesUtils as ExpressionSerdesUtils,
)
from gravitino.dto.util.dto_converters import DTOConverters


def _encode_default_value(
    default_value: Optional[Expression],
) -> Optional[Dict[str, Any]]:
    if default_value is None:
        return None
    return ExpressionSerdesUtils.write_function_arg(
        DTOConverters.to_function_arg(default_value)
    )


def _decode_default_value(
    default_value_dict: Optional[Dict[str, Any]],
) -> Optional[FunctionArg]:
    if default_value_dict is None:
        return None
    return ExpressionSerdesUtils.read_function_arg(default_value_dict)


@dataclass
class FunctionParamDTO(FunctionParam, DataClassJsonMixin):
    """DTO for function parameter."""

    _name: str = field(metadata=config(field_name="name"))
    _data_type: Type = field(
        metadata=config(
            field_name="dataType",
            encoder=TypeSerdes.serialize,
            decoder=TypeSerdes.deserialize,
        )
    )
    _comment: Optional[str] = field(default=None, metadata=config(field_name="comment"))
    _default_value: Optional[FunctionArg] = field(
        default=None,
        metadata=config(
            field_name="defaultValue",
            encoder=_encode_default_value,
            decoder=_decode_default_value,
        ),
    )

    def name(self) -> str:
        """Returns the parameter name."""
        return self._name

    def data_type(self) -> Type:
        """Returns the parameter data type."""
        return self._data_type

    def comment(self) -> Optional[str]:
        """Returns the optional parameter comment."""
        return self._comment

    def default_value(self) -> Optional[Expression]:
        """Returns the default value of the parameter if provided, otherwise None."""
        if self._default_value is None:
            return None
        return DTOConverters.from_function_arg(self._default_value)

    def to_function_param(self) -> FunctionParam:
        """Convert this DTO to a FunctionParam instance."""
        return FunctionParams.of(
            self._name, self._data_type, self._comment, self.default_value()
        )

    @classmethod
    def from_function_param(cls, param: FunctionParam) -> "FunctionParamDTO":
        """Create a FunctionParamDTO from a FunctionParam instance."""
        default_value = param.default_value()
        return cls(
            _name=param.name(),
            _data_type=param.data_type(),
            _comment=param.comment(),
            _default_value=(
                None
                if default_value is None
                else DTOConverters.to_function_arg(default_value)
            ),
        )

    def __eq__(self, other) -> bool:
        if not isinstance(other, FunctionParamDTO):
            return False
        return (
            self._name == other._name
            and self._data_type == other._data_type
            and self._comment == other._comment
            and self._default_value == other._default_value
        )

    def __hash__(self) -> int:
        return hash((self._name, self._data_type, self._comment, self._default_value))
