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


class SQLImpl(FunctionImpl):
    """SQL implementation with runtime and SQL body."""

    def __init__(
        self,
        runtime: FunctionImpl.RuntimeType,
        sql: str,
        resources: Optional[FunctionResources] = None,
        properties: Optional[Dict[str, str]] = None,
    ):
        """Create a SQLImpl instance.

        Args:
            runtime: The runtime of the function implementation.
            sql: The SQL that defines the function.
            resources: The resources required by the function implementation.
            properties: The properties of the function implementation.

        Raises:
            ValueError: If sql is null or empty.
        """
        super().__init__(FunctionImpl.Language.SQL, runtime, resources, properties)
        if not sql or not sql.strip():
            raise ValueError("SQL text cannot be null or empty")
        self._sql = sql

    def sql(self) -> str:
        """Returns the SQL that defines the function."""
        return self._sql

    @staticmethod
    def builder() -> "SQLImpl.Builder":
        """Returns a new Builder for SQLImpl."""
        return SQLImpl.Builder()

    def __eq__(self, other) -> bool:
        if not isinstance(other, SQLImpl):
            return False
        return (
            self.language() == other.language()
            and self.runtime() == other.runtime()
            and self.resources() == other.resources()
            and self.properties() == other.properties()
            and self._sql == other._sql
        )

    def __hash__(self) -> int:
        return hash(
            (
                self.language(),
                self.runtime(),
                self.resources(),
                tuple(sorted(self.properties().items())),
                self._sql,
            )
        )

    def __repr__(self) -> str:
        return (
            f"SQLImpl(language={self.language()}, runtime={self.runtime()}, "
            f"sql='{self._sql}', resources={self.resources()}, "
            f"properties={self.properties()})"
        )

    class Builder:
        """Builder for SQLImpl."""

        def __init__(self):
            self._runtime: Optional[FunctionImpl.RuntimeType] = None
            self._sql: Optional[str] = None
            self._resources: Optional[FunctionResources] = None
            self._properties: Optional[Dict[str, str]] = None

        def with_runtime_type(
            self, runtime: FunctionImpl.RuntimeType
        ) -> "SQLImpl.Builder":
            """Sets the runtime type.

            Args:
                runtime: The runtime of the function implementation.

            Returns:
                The builder instance.
            """
            self._runtime = runtime
            return self

        def with_sql(self, sql: str) -> "SQLImpl.Builder":
            """Sets the SQL text.

            Args:
                sql: The SQL that defines the function.

            Returns:
                The builder instance.
            """
            self._sql = sql
            return self

        def with_resources(self, resources: FunctionResources) -> "SQLImpl.Builder":
            """Sets the resources.

            Args:
                resources: The resources required by the function implementation.

            Returns:
                The builder instance.
            """
            self._resources = resources
            return self

        def with_properties(self, properties: Dict[str, str]) -> "SQLImpl.Builder":
            """Sets the properties.

            Args:
                properties: The properties of the function implementation.

            Returns:
                The builder instance.
            """
            self._properties = properties
            return self

        def build(self) -> "SQLImpl":
            """Builds a SQLImpl instance.

            Returns:
                A new SQLImpl instance.

            Raises:
                IllegalArgumentException: If required fields are not set.
            """
            Precondition.check_argument(
                self._runtime is not None, "Runtime type cannot be null"
            )
            Precondition.check_argument(self._sql is not None, "SQL cannot be null")

            return SQLImpl(
                runtime=self._runtime,
                sql=self._sql,
                resources=self._resources,
                properties=self._properties,
            )
