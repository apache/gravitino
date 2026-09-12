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

from gravitino.utils.precondition import Precondition


class DialectExpression:
    """One dialect-specific rendering of a Semantic Model expression."""

    def __init__(self, dialect: str, expression: str):
        Precondition.check_argument(
            dialect is not None and dialect != "", "dialect must not be null or empty"
        )
        Precondition.check_argument(
            expression is not None and expression != "",
            "expression must not be null or empty",
        )
        self._dialect = dialect
        self._expression = expression

    def dialect(self) -> str:
        """Returns the dialect identifier, see :class:`Dialects`."""
        return self._dialect

    def expression(self) -> str:
        """Returns the expression rendered in this dialect."""
        return self._expression

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, DialectExpression):
            return False
        return (
            self._dialect == other.dialect() and self._expression == other.expression()
        )

    def __hash__(self) -> int:
        return hash((self._dialect, self._expression))

    def __repr__(self) -> str:
        return (
            f"DialectExpression(dialect={self._dialect!r}, "
            f"expression={self._expression!r})"
        )
