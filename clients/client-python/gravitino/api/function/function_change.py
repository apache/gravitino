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

from abc import ABC
from typing import List

from gravitino.api.function.function_definition import FunctionDefinition
from gravitino.api.function.function_impl import FunctionImpl
from gravitino.api.function.function_param import FunctionParam


class FunctionChange(ABC):
    """Represents a change that can be applied to a function."""

    EMPTY_PARAMS: List[FunctionParam] = []
    """An empty list of parameters."""

    @staticmethod
    def update_comment(new_comment: str) -> "FunctionChange":
        """Create a FunctionChange to update the comment of a function.

        Args:
            new_comment: The new comment value.

        Returns:
            The change instance.
        """
        return UpdateComment(new_comment)

    @staticmethod
    def add_definition(definition: FunctionDefinition) -> "FunctionChange":
        """Create a FunctionChange to add a new definition (overload) to a function.

        Args:
            definition: The new definition to add.

        Returns:
            The change instance.
        """
        return AddDefinition(definition)

    @staticmethod
    def remove_definition(parameters: List[FunctionParam]) -> "FunctionChange":
        """Create a FunctionChange to remove an existing definition from a function.

        Args:
            parameters: The parameters that identify the definition to remove.

        Returns:
            The change instance.
        """
        return RemoveDefinition(parameters)

    @staticmethod
    def add_impl(
        parameters: List[FunctionParam], implementation: FunctionImpl
    ) -> "FunctionChange":
        """Create a FunctionChange to add an implementation to a specific definition.

        Args:
            parameters: The parameters that identify the definition to update.
            implementation: The implementation to add.

        Returns:
            The change instance.
        """
        return AddImpl(parameters, implementation)

    @staticmethod
    def update_impl(
        parameters: List[FunctionParam],
        runtime: FunctionImpl.RuntimeType,
        implementation: FunctionImpl,
    ) -> "FunctionChange":
        """Create a FunctionChange to update an implementation for a specific definition.

        Args:
            parameters: The parameters that identify the definition to update.
            runtime: The runtime that identifies the implementation to replace.
            implementation: The new implementation.

        Returns:
            The change instance.
        """
        return UpdateImpl(parameters, runtime, implementation)

    @staticmethod
    def remove_impl(
        parameters: List[FunctionParam], runtime: FunctionImpl.RuntimeType
    ) -> "FunctionChange":
        """Create a FunctionChange to remove an implementation for a specific definition.

        Args:
            parameters: The parameters that identify the definition to update.
            runtime: The runtime that identifies the implementation to remove.

        Returns:
            The change instance.
        """
        return RemoveImpl(parameters, runtime)


class UpdateComment(FunctionChange):
    """A FunctionChange to update the comment of a function."""

    def __init__(self, new_comment: str):
        if not new_comment or not new_comment.strip():
            raise ValueError("New comment cannot be null or empty")
        self._new_comment = new_comment

    def new_comment(self) -> str:
        """Returns the new comment of the function."""
        return self._new_comment

    def __eq__(self, other) -> bool:
        if not isinstance(other, UpdateComment):
            return False
        return self._new_comment == other._new_comment

    def __hash__(self) -> int:
        return hash(self._new_comment)

    def __repr__(self) -> str:
        return f"UpdateComment(newComment='{self._new_comment}')"


class AddDefinition(FunctionChange):
    """A FunctionChange to add a new definition to a function."""

    def __init__(self, definition: FunctionDefinition):
        if definition is None:
            raise ValueError("Definition cannot be null")
        self._definition = definition

    def definition(self) -> FunctionDefinition:
        """Returns the definition to add."""
        return self._definition

    def __eq__(self, other) -> bool:
        if not isinstance(other, AddDefinition):
            return False
        return self._definition == other._definition

    def __hash__(self) -> int:
        return hash(self._definition)

    def __repr__(self) -> str:
        return f"AddDefinition(definition={self._definition})"


class RemoveDefinition(FunctionChange):
    """A FunctionChange to remove an existing definition from a function."""

    def __init__(self, parameters: List[FunctionParam]):
        if parameters is None:
            raise ValueError("Parameters cannot be null")
        self._parameters = list(parameters)

    def parameters(self) -> List[FunctionParam]:
        """Returns the parameters that identify the definition to remove."""
        return (
            list(self._parameters) if self._parameters else FunctionChange.EMPTY_PARAMS
        )

    def __eq__(self, other) -> bool:
        if not isinstance(other, RemoveDefinition):
            return False
        return self._parameters == other._parameters

    def __hash__(self) -> int:
        return hash(tuple(self._parameters))

    def __repr__(self) -> str:
        return f"RemoveDefinition(parameters={self._parameters})"


class AddImpl(FunctionChange):
    """A FunctionChange to add an implementation to a definition."""

    def __init__(self, parameters: List[FunctionParam], implementation: FunctionImpl):
        if parameters is None:
            raise ValueError("Parameters cannot be null")
        self._parameters = list(parameters)
        if implementation is None:
            raise ValueError("Implementation cannot be null")
        self._implementation = implementation

    def parameters(self) -> List[FunctionParam]:
        """Returns the parameters that identify the definition to update."""
        return (
            list(self._parameters) if self._parameters else FunctionChange.EMPTY_PARAMS
        )

    def implementation(self) -> FunctionImpl:
        """Returns the implementation to add."""
        return self._implementation

    def __eq__(self, other) -> bool:
        if not isinstance(other, AddImpl):
            return False
        return (
            self._parameters == other._parameters
            and self._implementation == other._implementation
        )

    def __hash__(self) -> int:
        return hash((tuple(self._parameters), self._implementation))

    def __repr__(self) -> str:
        return (
            f"AddImpl(parameters={self._parameters}, "
            f"implementation={self._implementation})"
        )


class UpdateImpl(FunctionChange):
    """A FunctionChange to replace an implementation for a specific definition."""

    def __init__(
        self,
        parameters: List[FunctionParam],
        runtime: FunctionImpl.RuntimeType,
        implementation: FunctionImpl,
    ):
        if parameters is None:
            raise ValueError("Parameters cannot be null")
        self._parameters = list(parameters)
        if runtime is None:
            raise ValueError("Runtime cannot be null")
        self._runtime = runtime
        if implementation is None:
            raise ValueError("Implementation cannot be null")
        self._implementation = implementation
        if runtime != implementation.runtime():
            raise ValueError(
                "Runtime of implementation must match the runtime being updated"
            )

    def parameters(self) -> List[FunctionParam]:
        """Returns the parameters that identify the definition to update."""
        return (
            list(self._parameters) if self._parameters else FunctionChange.EMPTY_PARAMS
        )

    def runtime(self) -> FunctionImpl.RuntimeType:
        """Returns the runtime that identifies the implementation to replace."""
        return self._runtime

    def implementation(self) -> FunctionImpl:
        """Returns the new implementation."""
        return self._implementation

    def __eq__(self, other) -> bool:
        if not isinstance(other, UpdateImpl):
            return False
        return (
            self._parameters == other._parameters
            and self._runtime == other._runtime
            and self._implementation == other._implementation
        )

    def __hash__(self) -> int:
        return hash((tuple(self._parameters), self._runtime, self._implementation))

    def __repr__(self) -> str:
        return (
            f"UpdateImpl(parameters={self._parameters}, runtime={self._runtime}, "
            f"implementation={self._implementation})"
        )


class RemoveImpl(FunctionChange):
    """A FunctionChange to remove an implementation for a specific runtime."""

    def __init__(
        self, parameters: List[FunctionParam], runtime: FunctionImpl.RuntimeType
    ):
        if parameters is None:
            raise ValueError("Parameters cannot be null")
        self._parameters = list(parameters)
        if runtime is None:
            raise ValueError("Runtime cannot be null")
        self._runtime = runtime

    def parameters(self) -> List[FunctionParam]:
        """Returns the parameters that identify the definition to update."""
        return (
            list(self._parameters) if self._parameters else FunctionChange.EMPTY_PARAMS
        )

    def runtime(self) -> FunctionImpl.RuntimeType:
        """Returns the runtime that identifies the implementation to remove."""
        return self._runtime

    def __eq__(self, other) -> bool:
        if not isinstance(other, RemoveImpl):
            return False
        return self._parameters == other._parameters and self._runtime == other._runtime

    def __hash__(self) -> int:
        return hash((tuple(self._parameters), self._runtime))

    def __repr__(self) -> str:
        return f"RemoveImpl(parameters={self._parameters}, runtime={self._runtime})"
