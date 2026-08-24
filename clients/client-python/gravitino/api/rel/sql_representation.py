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

from dataclasses import dataclass

from gravitino.api.rel.representation import Representation
from gravitino.utils.precondition import Precondition


@dataclass(frozen=True)
class SQLRepresentation(Representation):
    """A SQL-based representation of a view definition."""

    _dialect: str
    _sql: str

    def __post_init__(self):
        Precondition.check_string_not_empty(
            self._dialect, "dialect must not be null or empty"
        )
        Precondition.check_string_not_empty(self._sql, "sql must not be null or empty")

    def type(self) -> str:
        """Returns the representation type."""
        return Representation.TYPE_SQL

    def dialect(self) -> str:
        """Returns the SQL dialect."""
        return self._dialect

    def sql(self) -> str:
        """Returns the SQL text."""
        return self._sql
