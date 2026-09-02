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

from gravitino.api.semantic.expression import Expression
from gravitino.dto.semantic.dialect_expression_dto import DialectExpressionDTO
from gravitino.dto.semantic.semantic_dto_utils import convert_list, is_none
from gravitino.utils.precondition import Precondition


@dataclass
class ExpressionDTO(DataClassJsonMixin):
    """Represents a Semantic Model expression DTO."""

    _dialects: Optional[list[DialectExpressionDTO]] = field(
        default=None, metadata=config(field_name="dialects", exclude=is_none)
    )

    def dialects(self) -> Optional[list[DialectExpressionDTO]]:
        """Returns the dialect-specific renderings of this expression."""
        return self._dialects

    @staticmethod
    def from_expression(expression: Expression) -> "ExpressionDTO":
        """Convert an expression to its DTO."""
        return ExpressionDTO(
            _dialects=convert_list(
                expression.dialects(), DialectExpressionDTO.from_dialect_expression
            )
        )

    def to_expression(self) -> Expression:
        """Convert this DTO to an expression."""
        Precondition.check_argument(
            self._dialects is not None, "dialects must not be null"
        )
        return Expression(
            convert_list(self._dialects, DialectExpressionDTO.to_dialect_expression)
        )
