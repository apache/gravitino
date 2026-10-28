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

from typing import Optional

from gravitino.utils.precondition import Precondition


class SemanticQueryPlan:
    """The SQL produced for a `SemanticQuery`, with the dialect and planner used.

    The server compiles a plan without executing it; the SQL is returned to the
    caller and is not cached.
    """

    def __init__(
        self,
        sql: str,
        dialect: str,
        planner: str,
        warnings: Optional[list[str]] = None,
    ):
        Precondition.check_string_not_empty(sql, "sql must not be null or empty")
        Precondition.check_string_not_empty(
            dialect, "dialect must not be null or empty"
        )
        Precondition.check_string_not_empty(
            planner, "planner must not be null or empty"
        )
        warnings = [] if warnings is None else warnings
        for index, warning in enumerate(warnings):
            Precondition.check_string_not_empty(
                warning, f"warnings[{index}] must not be null or empty"
            )
        self._sql = sql
        self._dialect = dialect
        self._planner = planner
        self._warnings = tuple(warnings)

    def sql(self) -> str:
        """Returns the generated SQL."""
        return self._sql

    def dialect(self) -> str:
        """Returns the dialect the SQL targets."""
        return self._dialect

    def planner(self) -> str:
        """Returns the name of the planner that generated the SQL."""
        return self._planner

    def warnings(self) -> list[str]:
        """Returns non-fatal warnings produced while planning."""
        return list(self._warnings)

    def __eq__(self, other) -> bool:
        if not isinstance(other, SemanticQueryPlan):
            return False
        return (
            self._sql == other._sql
            and self._dialect == other._dialect
            and self._planner == other._planner
            and self._warnings == other._warnings
        )

    def __hash__(self) -> int:
        return hash((self._sql, self._dialect, self._planner, self._warnings))

    def __repr__(self) -> str:
        return (
            f"SemanticQueryPlan(sql={self._sql!r}, dialect={self._dialect!r}, "
            f"planner={self._planner!r}, warnings={list(self._warnings)!r})"
        )
