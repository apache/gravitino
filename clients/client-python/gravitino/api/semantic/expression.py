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

from gravitino.api.semantic.dialect_expression import DialectExpression
from gravitino.api.semantic.semantic_utils import check_no_none_elements
from gravitino.utils.precondition import Precondition


class Expression:
    """A Semantic Model expression rendered in one or more dialects.

    Every expression declares at least one dialect and each dialect identifier
    appears at most once.
    """

    def __init__(self, dialects: list[DialectExpression]):
        Precondition.check_argument(
            dialects is not None and len(dialects) > 0,
            "dialects must not be null or empty",
        )
        check_no_none_elements("dialects", dialects)

        seen_dialects = set()
        for dialect_expression in dialects:
            Precondition.check_argument(
                dialect_expression.dialect() not in seen_dialects,
                "dialects must not contain duplicate dialect: "
                f"{dialect_expression.dialect()}",
            )
            seen_dialects.add(dialect_expression.dialect())

        self._dialects = list(dialects)

    def dialects(self) -> list[DialectExpression]:
        """Returns the dialect-specific renderings of this expression."""
        return list(self._dialects)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Expression):
            return False
        return self._dialects == other.dialects()

    def __hash__(self) -> int:
        return hash(tuple(self._dialects))

    def __repr__(self) -> str:
        return f"Expression(dialects={self._dialects!r})"
