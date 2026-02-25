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

from typing import Dict, Optional

from gravitino.api.function.function_impl import FunctionImpl
from gravitino.api.function.function_resources import FunctionResources
from gravitino.utils.precondition import Precondition


class JavaImpl(FunctionImpl):
    """Java implementation with class name."""

    def __init__(
        self,
        runtime: FunctionImpl.RuntimeType,
        class_name: str,
        resources: Optional[FunctionResources] = None,
        properties: Optional[Dict[str, str]] = None,
    ):
        """Create a JavaImpl instance.

        Args:
            runtime: The runtime of the function implementation.
            class_name: The fully qualified class name.
            resources: The resources required by the function implementation.
            properties: The properties of the function implementation.

        Raises:
            ValueError: If class_name is null or empty.
        """
        super().__init__(FunctionImpl.Language.JAVA, runtime, resources, properties)
        if not class_name or not class_name.strip():
            raise ValueError("Java class name cannot be null or empty")
        self._class_name = class_name

    def class_name(self) -> str:
        """Returns the fully qualified class name."""
        return self._class_name

    @staticmethod
    def builder() -> "JavaImpl.Builder":
        """Returns a new Builder for JavaImpl."""
        return JavaImpl.Builder()

    def __eq__(self, other) -> bool:
        if not isinstance(other, JavaImpl):
            return False
        return (
            self.language() == other.language()
            and self.runtime() == other.runtime()
            and self.resources() == other.resources()
            and self.properties() == other.properties()
            and self._class_name == other._class_name
        )

    def __hash__(self) -> int:
        return hash(
            (
                self.language(),
                self.runtime(),
                self.resources(),
                tuple(sorted(self.properties().items())),
                self._class_name,
            )
        )

    def __repr__(self) -> str:
        return (
            f"JavaImpl(language={self.language()}, runtime={self.runtime()}, "
            f"className='{self._class_name}', resources={self.resources()}, "
            f"properties={self.properties()})"
        )

    class Builder:
        """Builder for JavaImpl."""

        def __init__(self):
            self._runtime: Optional[FunctionImpl.RuntimeType] = None
            self._class_name: Optional[str] = None
            self._resources: Optional[FunctionResources] = None
            self._properties: Optional[Dict[str, str]] = None

        def with_runtime_type(
            self, runtime: FunctionImpl.RuntimeType
        ) -> "JavaImpl.Builder":
            """Sets the runtime type.

            Args:
                runtime: The runtime of the function implementation.

            Returns:
                The builder instance.
            """
            self._runtime = runtime
            return self

        def with_class_name(self, class_name: str) -> "JavaImpl.Builder":
            """Sets the class name.

            Args:
                class_name: The fully qualified class name.

            Returns:
                The builder instance.
            """
            self._class_name = class_name
            return self

        def with_resources(self, resources: FunctionResources) -> "JavaImpl.Builder":
            """Sets the resources.

            Args:
                resources: The resources required by the function implementation.

            Returns:
                The builder instance.
            """
            self._resources = resources
            return self

        def with_properties(self, properties: Dict[str, str]) -> "JavaImpl.Builder":
            """Sets the properties.

            Args:
                properties: The properties of the function implementation.

            Returns:
                The builder instance.
            """
            self._properties = properties
            return self

        def build(self) -> "JavaImpl":
            """Builds a JavaImpl instance.

            Returns:
                A new JavaImpl instance.

            Raises:
                IllegalArgumentException: If required fields are not set.
            """
            Precondition.check_argument(
                self._runtime is not None, "Runtime type cannot be null"
            )
            Precondition.check_argument(
                self._class_name is not None, "Class name cannot be null"
            )

            return JavaImpl(
                runtime=self._runtime,
                class_name=self._class_name,
                resources=self._resources,
                properties=self._properties,
            )
