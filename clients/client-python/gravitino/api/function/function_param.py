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

from abc import ABC, abstractmethod
from typing import Optional

from gravitino.api.rel.expressions.expression import Expression
from gravitino.api.rel.types.type import Type


class FunctionParam(ABC):
    """Represents a function parameter."""

    @abstractmethod
    def name(self) -> str:
        """Returns the name of the parameter."""
        pass

    @abstractmethod
    def data_type(self) -> Type:
        """Returns the data type of the parameter."""
        pass

    def comment(self) -> Optional[str]:
        """Returns the optional comment of the parameter, None if not provided."""
        return None

    def default_value(self) -> Optional[Expression]:
        """Returns the default value of the parameter if provided, otherwise None."""
        return None


class FunctionParams:
    """Factory class for creating FunctionParam instances."""

    class SimpleFunctionParam(FunctionParam):
        """Simple implementation of FunctionParam."""

        def __init__(
            self,
            name: str,
            data_type: Type,
            comment: Optional[str] = None,
            default_value: Optional[Expression] = None,
        ):
            if not name or not name.strip():
                raise ValueError("Parameter name cannot be null or empty")
            self._name = name

            if data_type is None:
                raise ValueError("Parameter data type cannot be null")
            self._data_type = data_type

            self._comment = comment
            self._default_value = default_value

        def name(self) -> str:
            return self._name

        def data_type(self) -> Type:
            return self._data_type

        def comment(self) -> Optional[str]:
            return self._comment

        def default_value(self) -> Optional[Expression]:
            return self._default_value

        def __eq__(self, other) -> bool:
            if not isinstance(other, FunctionParam):
                return False
            return (
                self._name == other.name()
                and self._data_type == other.data_type()
                and self._comment == other.comment()
                and self._default_value == other.default_value()
            )

        def __hash__(self) -> int:
            return hash(
                (self._name, self._data_type, self._comment, self._default_value)
            )

        def __repr__(self) -> str:
            return (
                f"FunctionParam(name='{self._name}', dataType={self._data_type}, "
                f"comment='{self._comment}', defaultValue={self._default_value})"
            )

    @classmethod
    def of(
        cls,
        name: str,
        data_type: Type,
        comment: Optional[str] = None,
        default_value: Optional[Expression] = None,
    ) -> FunctionParam:
        """Create a FunctionParam instance.

        Args:
            name: The parameter name.
            data_type: The parameter type.
            comment: The optional comment.
            default_value: The optional default value.

        Returns:
            A FunctionParam instance.
        """
        return cls.SimpleFunctionParam(name, data_type, comment, default_value)
