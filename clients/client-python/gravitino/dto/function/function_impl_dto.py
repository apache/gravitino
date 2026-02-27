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
from typing import Dict, Optional

from dataclasses_json import DataClassJsonMixin, config

from gravitino.api.function.function_impl import FunctionImpl
from gravitino.api.function.java_impl import JavaImpl
from gravitino.api.function.python_impl import PythonImpl
from gravitino.api.function.sql_impl import SQLImpl
from gravitino.dto.function.function_resources_dto import FunctionResourcesDTO


class FunctionImplDTO(DataClassJsonMixin, ABC):
    """Abstract DTO for function implementation."""

    @abstractmethod
    def language(self) -> FunctionImpl.Language:
        """Returns the language of this implementation."""
        pass

    @abstractmethod
    def to_function_impl(self) -> FunctionImpl:
        """Convert this DTO to a FunctionImpl instance."""
        pass


@dataclass
class SQLImplDTO(FunctionImplDTO):
    """DTO for SQL function implementation."""

    _runtime: str = field(metadata=config(field_name="runtime"))
    _sql: str = field(metadata=config(field_name="sql"))
    _language: str = field(default="SQL", metadata=config(field_name="language"))
    _resources: Optional[FunctionResourcesDTO] = field(
        default=None, metadata=config(field_name="resources")
    )
    _properties: Optional[Dict[str, str]] = field(
        default=None, metadata=config(field_name="properties")
    )

    def language(self) -> FunctionImpl.Language:
        return FunctionImpl.Language.SQL

    def runtime(self) -> str:
        return self._runtime

    def sql(self) -> str:
        return self._sql

    def resources(self) -> Optional[FunctionResourcesDTO]:
        return self._resources

    def properties(self) -> Optional[Dict[str, str]]:
        return self._properties

    def to_function_impl(self) -> FunctionImpl:
        return SQLImpl(
            runtime=FunctionImpl.RuntimeType.from_string(self._runtime),
            sql=self._sql,
            resources=(
                self._resources.to_function_resources() if self._resources else None
            ),
            properties=self._properties,
        )

    @classmethod
    def from_function_impl(cls, impl) -> "SQLImplDTO":
        """Create a SQLImplDTO from a SQLImpl instance."""
        return cls(
            _runtime=impl.runtime().name,
            _sql=impl.sql(),
            _resources=FunctionResourcesDTO.from_function_resources(impl.resources()),
            _properties=impl.properties() if impl.properties() else None,
        )

    def __eq__(self, other) -> bool:
        if not isinstance(other, SQLImplDTO):
            return False
        return (
            self._runtime == other._runtime
            and self._sql == other._sql
            and self._resources == other._resources
            and self._properties == other._properties
        )

    def __hash__(self) -> int:
        return hash(
            (
                self._runtime,
                self._sql,
                self._resources,
                frozenset(self._properties.items()) if self._properties else None,
            )
        )


@dataclass
class JavaImplDTO(FunctionImplDTO):
    """DTO for Java function implementation."""

    _runtime: str = field(metadata=config(field_name="runtime"))
    _class_name: str = field(metadata=config(field_name="className"))
    _language: str = field(default="JAVA", metadata=config(field_name="language"))
    _resources: Optional[FunctionResourcesDTO] = field(
        default=None, metadata=config(field_name="resources")
    )
    _properties: Optional[Dict[str, str]] = field(
        default=None, metadata=config(field_name="properties")
    )

    def language(self) -> FunctionImpl.Language:
        return FunctionImpl.Language.JAVA

    def runtime(self) -> str:
        return self._runtime

    def class_name(self) -> str:
        return self._class_name

    def resources(self) -> Optional[FunctionResourcesDTO]:
        return self._resources

    def properties(self) -> Optional[Dict[str, str]]:
        return self._properties

    def to_function_impl(self) -> FunctionImpl:
        return JavaImpl(
            runtime=FunctionImpl.RuntimeType.from_string(self._runtime),
            class_name=self._class_name,
            resources=(
                self._resources.to_function_resources() if self._resources else None
            ),
            properties=self._properties,
        )

    @classmethod
    def from_function_impl(cls, impl) -> "JavaImplDTO":
        """Create a JavaImplDTO from a JavaImpl instance."""
        return cls(
            _runtime=impl.runtime().name,
            _class_name=impl.class_name(),
            _resources=FunctionResourcesDTO.from_function_resources(impl.resources()),
            _properties=impl.properties() if impl.properties() else None,
        )

    def __eq__(self, other) -> bool:
        if not isinstance(other, JavaImplDTO):
            return False
        return (
            self._runtime == other._runtime
            and self._class_name == other._class_name
            and self._resources == other._resources
            and self._properties == other._properties
        )

    def __hash__(self) -> int:
        return hash(
            (
                self._runtime,
                self._class_name,
                self._resources,
                frozenset(self._properties.items()) if self._properties else None,
            )
        )


@dataclass
class PythonImplDTO(FunctionImplDTO):
    """DTO for Python function implementation."""

    _runtime: str = field(metadata=config(field_name="runtime"))
    _handler: str = field(metadata=config(field_name="handler"))
    _language: str = field(default="PYTHON", metadata=config(field_name="language"))
    _code_block: Optional[str] = field(
        default=None, metadata=config(field_name="codeBlock")
    )
    _resources: Optional[FunctionResourcesDTO] = field(
        default=None, metadata=config(field_name="resources")
    )
    _properties: Optional[Dict[str, str]] = field(
        default=None, metadata=config(field_name="properties")
    )

    def language(self) -> FunctionImpl.Language:
        return FunctionImpl.Language.PYTHON

    def runtime(self) -> str:
        return self._runtime

    def handler(self) -> str:
        return self._handler

    def code_block(self) -> Optional[str]:
        return self._code_block

    def resources(self) -> Optional[FunctionResourcesDTO]:
        return self._resources

    def properties(self) -> Optional[Dict[str, str]]:
        return self._properties

    def to_function_impl(self) -> FunctionImpl:
        return PythonImpl(
            runtime=FunctionImpl.RuntimeType.from_string(self._runtime),
            handler=self._handler,
            code_block=self._code_block,
            resources=(
                self._resources.to_function_resources() if self._resources else None
            ),
            properties=self._properties,
        )

    @classmethod
    def from_function_impl(cls, impl) -> "PythonImplDTO":
        """Create a PythonImplDTO from a PythonImpl instance."""
        return cls(
            _runtime=impl.runtime().name,
            _handler=impl.handler(),
            _code_block=impl.code_block(),
            _resources=FunctionResourcesDTO.from_function_resources(impl.resources()),
            _properties=impl.properties() if impl.properties() else None,
        )

    def __eq__(self, other) -> bool:
        if not isinstance(other, PythonImplDTO):
            return False
        return (
            self._runtime == other._runtime
            and self._handler == other._handler
            and self._code_block == other._code_block
            and self._resources == other._resources
            and self._properties == other._properties
        )

    def __hash__(self) -> int:
        return hash(
            (
                self._runtime,
                self._handler,
                self._code_block,
                self._resources,
                frozenset(self._properties.items()) if self._properties else None,
            )
        )


def function_impl_dto_from_function_impl(impl: FunctionImpl) -> FunctionImplDTO:
    """Create a FunctionImplDTO from a FunctionImpl instance."""
    if isinstance(impl, SQLImpl):
        return SQLImplDTO.from_function_impl(impl)
    if isinstance(impl, JavaImpl):
        return JavaImplDTO.from_function_impl(impl)
    if isinstance(impl, PythonImpl):
        return PythonImplDTO.from_function_impl(impl)

    raise ValueError(f"Unsupported implementation type: {type(impl)}")
