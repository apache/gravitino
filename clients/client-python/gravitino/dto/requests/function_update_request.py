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
from dataclasses import dataclass, field
from typing import List

from dataclasses_json import DataClassJsonMixin, config

from gravitino.api.function.function_change import FunctionChange
from gravitino.api.function.function_impl import FunctionImpl
from gravitino.dto.function.function_definition_dto import FunctionDefinitionDTO
from gravitino.dto.function.function_impl_dto import FunctionImplDTO
from gravitino.dto.function.function_param_dto import FunctionParamDTO
from gravitino.exceptions.base import IllegalArgumentException
from gravitino.rest.rest_message import RESTRequest


@dataclass
class FunctionUpdateRequest(RESTRequest, ABC):
    """Abstract base class for function update requests."""

    _type: str = field(metadata=config(field_name="@type"))

    def __init__(self, action_type: str):
        self._type = action_type

    @abstractmethod
    def function_change(self) -> FunctionChange:
        """Returns the function change."""
        pass


@dataclass
class UpdateCommentRequest(FunctionUpdateRequest, DataClassJsonMixin):
    """Request to update the comment of a function."""

    _new_comment: str = field(metadata=config(field_name="newComment"))

    def __init__(self, new_comment: str):
        super().__init__("updateComment")
        self._new_comment = new_comment

    def function_change(self) -> FunctionChange:
        return FunctionChange.update_comment(self._new_comment)

    def validate(self):
        pass


@dataclass
class AddDefinitionRequest(FunctionUpdateRequest, DataClassJsonMixin):
    """Request to add a definition to a function."""

    _definition: FunctionDefinitionDTO = field(metadata=config(field_name="definition"))

    def __init__(self, definition: FunctionDefinitionDTO):
        super().__init__("addDefinition")
        self._definition = definition

    def function_change(self) -> FunctionChange:
        return FunctionChange.add_definition(self._definition.to_function_definition())

    def validate(self):
        if self._definition is None:
            raise IllegalArgumentException(
                "'definition' field is required and cannot be null"
            )


@dataclass
class RemoveDefinitionRequest(FunctionUpdateRequest, DataClassJsonMixin):
    """Request to remove a definition from a function."""

    _parameters: List[FunctionParamDTO] = field(
        metadata=config(field_name="parameters")
    )

    def __init__(self, parameters: List[FunctionParamDTO]):
        super().__init__("removeDefinition")
        self._parameters = parameters

    def function_change(self) -> FunctionChange:
        params = (
            [p.to_function_param() for p in self._parameters]
            if self._parameters
            else []
        )
        return FunctionChange.remove_definition(params)

    def validate(self):
        if self._parameters is None:
            raise IllegalArgumentException(
                "'parameters' field is required and cannot be null"
            )


@dataclass
class AddImplRequest(FunctionUpdateRequest, DataClassJsonMixin):
    """Request to add an implementation to a definition."""

    _parameters: List[FunctionParamDTO] = field(
        metadata=config(field_name="parameters")
    )
    _implementation: FunctionImplDTO = field(
        metadata=config(field_name="implementation")
    )

    def __init__(
        self, parameters: List[FunctionParamDTO], implementation: FunctionImplDTO
    ):
        super().__init__("addImpl")
        self._parameters = parameters
        self._implementation = implementation

    def function_change(self) -> FunctionChange:
        params = (
            [p.to_function_param() for p in self._parameters]
            if self._parameters
            else []
        )
        return FunctionChange.add_impl(params, self._implementation.to_function_impl())

    def validate(self):
        if self._parameters is None:
            raise IllegalArgumentException(
                "'parameters' field is required and cannot be null"
            )
        if self._implementation is None:
            raise IllegalArgumentException(
                "'implementation' field is required and cannot be null"
            )


@dataclass
class UpdateImplRequest(FunctionUpdateRequest, DataClassJsonMixin):
    """Request to update an implementation in a definition."""

    _parameters: List[FunctionParamDTO] = field(
        metadata=config(field_name="parameters")
    )
    _runtime: str = field(metadata=config(field_name="runtime"))
    _implementation: FunctionImplDTO = field(
        metadata=config(field_name="implementation")
    )

    def __init__(
        self,
        parameters: List[FunctionParamDTO],
        runtime: str,
        implementation: FunctionImplDTO,
    ):
        super().__init__("updateImpl")
        self._parameters = parameters
        self._runtime = runtime
        self._implementation = implementation

    def function_change(self) -> FunctionChange:

        params = (
            [p.to_function_param() for p in self._parameters]
            if self._parameters
            else []
        )
        runtime_type = FunctionImpl.RuntimeType.from_string(self._runtime)
        return FunctionChange.update_impl(
            params, runtime_type, self._implementation.to_function_impl()
        )

    def validate(self):
        if self._parameters is None:
            raise IllegalArgumentException(
                "'parameters' field is required and cannot be null"
            )
        if self._runtime is None:
            raise IllegalArgumentException(
                "'runtime' field is required and cannot be null"
            )
        if self._implementation is None:
            raise IllegalArgumentException(
                "'implementation' field is required and cannot be null"
            )


@dataclass
class RemoveImplRequest(FunctionUpdateRequest, DataClassJsonMixin):
    """Request to remove an implementation from a definition."""

    _parameters: List[FunctionParamDTO] = field(
        metadata=config(field_name="parameters")
    )
    _runtime: str = field(metadata=config(field_name="runtime"))

    def __init__(self, parameters: List[FunctionParamDTO], runtime: str):
        super().__init__("removeImpl")
        self._parameters = parameters
        self._runtime = runtime

    def function_change(self) -> FunctionChange:

        params = (
            [p.to_function_param() for p in self._parameters]
            if self._parameters
            else []
        )
        runtime_type = FunctionImpl.RuntimeType.from_string(self._runtime)
        return FunctionChange.remove_impl(params, runtime_type)

    def validate(self):
        if self._parameters is None:
            raise IllegalArgumentException(
                "'parameters' field is required and cannot be null"
            )
        if self._runtime is None:
            raise IllegalArgumentException(
                "'runtime' field is required and cannot be null"
            )
