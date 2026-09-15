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

from dataclasses import dataclass, field
from typing import Optional

from dataclasses_json import DataClassJsonMixin, config

from gravitino.api.semantic.dialect_expression import DialectExpression
from gravitino.dto.semantic.semantic_dto_utils import is_none


@dataclass
class DialectExpressionDTO(DataClassJsonMixin):
    """Represents one dialect-specific rendering of an expression."""

    _dialect: Optional[str] = field(
        default=None, metadata=config(field_name="dialect", exclude=is_none)
    )
    _expression: Optional[str] = field(
        default=None, metadata=config(field_name="expression", exclude=is_none)
    )

    def dialect(self) -> Optional[str]:
        """Returns the dialect identifier."""
        return self._dialect

    def expression(self) -> Optional[str]:
        """Returns the expression rendered in this dialect."""
        return self._expression

    @staticmethod
    def from_dialect_expression(
        dialect_expression: DialectExpression,
    ) -> "DialectExpressionDTO":
        """Convert a dialect expression to its DTO."""
        return DialectExpressionDTO(
            _dialect=dialect_expression.dialect(),
            _expression=dialect_expression.expression(),
        )

    def to_dialect_expression(self) -> DialectExpression:
        """Convert this DTO to a dialect expression."""
        return DialectExpression(self._dialect, self._expression)
