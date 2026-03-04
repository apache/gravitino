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
from enum import Enum
from typing import Dict, Optional

from gravitino.api.function.function_resources import FunctionResources


class FunctionImpl(ABC):
    """Base class of function implementations.

    A function implementation must declare its language and optional external resources.
    Concrete implementations are provided by SQLImpl, JavaImpl, and PythonImpl.
    """

    class Language(Enum):
        """Supported implementation languages."""

        SQL = "SQL"
        """SQL implementation."""

        JAVA = "JAVA"
        """Java implementation."""

        PYTHON = "PYTHON"
        """Python implementation."""

    class RuntimeType(Enum):
        """Supported execution runtimes for function implementations."""

        SPARK = "SPARK"
        """Spark runtime."""

        TRINO = "TRINO"
        """Trino runtime."""

        @classmethod
        def from_string(cls, value: str) -> "FunctionImpl.RuntimeType":
            """Parse a runtime value from string.

            Args:
                value: Runtime name.

            Returns:
                Parsed runtime.

            Raises:
                ValueError: If the runtime is not supported.
            """
            if not value or not value.strip():
                raise ValueError("Function runtime must be set")
            normalized = value.strip().upper()
            for runtime in cls:
                if runtime.name == normalized:
                    return runtime
            raise ValueError(f"Unsupported function runtime: {value}")

    def __init__(
        self,
        language: Language,
        runtime: RuntimeType,
        resources: Optional[FunctionResources] = None,
        properties: Optional[Dict[str, str]] = None,
    ):
        """Construct a FunctionImpl.

        Args:
            language: The language of the function implementation.
            runtime: The runtime of the function implementation.
            resources: The resources required by the function implementation.
            properties: The properties of the function implementation.

        Raises:
            ValueError: If language or runtime is not set.
        """
        if language is None:
            raise ValueError("Function implementation language must be set")
        self._language = language

        if runtime is None:
            raise ValueError("Function runtime must be set")
        self._runtime = runtime

        self._resources = resources if resources else FunctionResources.empty()
        self._properties = dict(properties) if properties else {}

    def language(self) -> Language:
        """Returns the implementation language."""
        return self._language

    def runtime(self) -> RuntimeType:
        """Returns the target runtime."""
        return self._runtime

    def resources(self) -> FunctionResources:
        """Returns the external resources required by this implementation."""
        return self._resources

    def properties(self) -> Dict[str, str]:
        """Returns the additional properties of this implementation."""
        return dict(self._properties)
