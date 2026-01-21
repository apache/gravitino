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
