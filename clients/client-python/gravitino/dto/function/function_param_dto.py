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
from typing import Optional

from dataclasses_json import DataClassJsonMixin, config

from gravitino.api.function.function_param import FunctionParam, FunctionParams
from gravitino.api.rel.types.json_serdes.type_serdes import TypeSerdes
from gravitino.api.rel.types.type import Type


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
    # Note: defaultValue handling is complex and may need custom serialization
    # For now, we'll keep it simple without expression support

    def name(self) -> str:
        """Returns the parameter name."""
        return self._name

    def data_type(self) -> Type:
        """Returns the parameter data type."""
        return self._data_type

    def comment(self) -> Optional[str]:
        """Returns the optional parameter comment."""
        return self._comment

    def to_function_param(self) -> FunctionParam:
        """Convert this DTO to a FunctionParam instance."""
        return FunctionParams.of(self._name, self._data_type, self._comment, None)

    @classmethod
    def from_function_param(cls, param: FunctionParam) -> "FunctionParamDTO":
        """Create a FunctionParamDTO from a FunctionParam instance."""
        return cls(
            _name=param.name(),
            _data_type=param.data_type(),
            _comment=param.comment(),
        )

    def __eq__(self, other) -> bool:
        if not isinstance(other, FunctionParamDTO):
            return False
        return (
            self._name == other._name
            and self._data_type == other._data_type
            and self._comment == other._comment
        )

    def __hash__(self) -> int:
        return hash((self._name, self._data_type, self._comment))
