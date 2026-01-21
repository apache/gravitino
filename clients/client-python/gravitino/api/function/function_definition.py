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
from typing import List

from gravitino.api.function.function_impl import FunctionImpl
from gravitino.api.function.function_param import FunctionParam


class FunctionDefinition(ABC):
    """A function definition that pairs a specific parameter list with its implementations.

    A single function can include multiple definitions (overloads), each with distinct
    parameters and implementations.
    """

    @abstractmethod
    def parameters(self) -> List[FunctionParam]:
        """Returns the parameters for this definition.

        Returns:
            The parameters for this definition. May be an empty list for a no-arg definition.
        """
        pass

    @abstractmethod
    def impls(self) -> List[FunctionImpl]:
        """Returns the implementations associated with this definition."""
        pass


class FunctionDefinitions:
    """Factory class for creating FunctionDefinition instances."""

    class SimpleFunctionDefinition(FunctionDefinition):
        """Simple implementation of FunctionDefinition."""

        def __init__(self, parameters: List[FunctionParam], impls: List[FunctionImpl]):
            self._parameters = list(parameters) if parameters else []
            self._impls = list(impls) if impls else []

        def parameters(self) -> List[FunctionParam]:
            return list(self._parameters)

        def impls(self) -> List[FunctionImpl]:
            return list(self._impls)

        def __eq__(self, other) -> bool:
            if not isinstance(other, FunctionDefinition):
                return False
            return self._parameters == list(other.parameters()) and self._impls == list(
                other.impls()
            )

        def __hash__(self) -> int:
            return hash((tuple(self._parameters), tuple(self._impls)))

        def __repr__(self) -> str:
            return f"FunctionDefinition(parameters={self._parameters}, impls={self._impls})"

    @classmethod
    def of(
        cls, parameters: List[FunctionParam], impls: List[FunctionImpl]
    ) -> FunctionDefinition:
        """Create a FunctionDefinition instance.

        Args:
            parameters: The parameters for this definition.
            impls: The implementations for this definition.

        Returns:
            A FunctionDefinition instance.
        """
        return cls.SimpleFunctionDefinition(parameters, impls)
