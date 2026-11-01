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

from dataclasses_json import config

from gravitino.api.semantic.data_type import DataType
from gravitino.api.semantic.dialect_expression import DialectExpression
from gravitino.api.semantic.expression import Expression
from gravitino.api.semantic.plan_field_ref import PlanFieldRef
from gravitino.dto.semantic.json_serdes.semantic_json_mixin import SemanticJsonMixin
from gravitino.dto.semantic.semantic_dto_utils import is_none
from gravitino.exceptions.base import IllegalArgumentException
from gravitino.utils.precondition import Precondition


@dataclass
class PlanFieldRefDTO(SemanticJsonMixin):
    """Represents a reference to a field of a dataset in a query plan."""

    _dataset: Optional[str] = field(default=None, metadata=config(field_name="dataset"))
    _expression: Optional[dict[str, str]] = field(
        default=None, metadata=config(field_name="expression", exclude=is_none)
    )
    _column: Optional[str] = field(
        default=None, metadata=config(field_name="column", exclude=is_none)
    )
    _datatype: Optional[str] = field(
        default=None, metadata=config(field_name="datatype", exclude=is_none)
    )

    def validate(self) -> None:
        """Validates the field reference.

        Raises:
            IllegalArgumentException: If the reference is not well formed.
        """
        Precondition.check_string_not_empty(
            self._dataset, '"dataset" field is required and cannot be empty'
        )
        Precondition.check_argument(
            (self._expression is None) != (self._column is None),
            'exactly one of "expression" and "column" must be set',
        )
        Precondition.check_argument(
            self._column is None or self._column.strip() != "",
            '"column" field cannot be empty when present',
        )
        if self._expression is not None:
            Precondition.check_argument(
                len(self._expression) > 0,
                '"expression" field cannot be empty when present',
            )
            for dialect, expression in self._expression.items():
                Precondition.check_string_not_empty(
                    dialect, '"expression" dialect names cannot be null or empty'
                )
                Precondition.check_string_not_empty(
                    expression,
                    f"\"expression\" for dialect '{dialect}' cannot be null or empty",
                )
        if self._datatype is not None:
            self._parse_datatype()

    def to_plan_field_ref(self) -> PlanFieldRef:
        """Converts this DTO to an API field reference."""
        self.validate()
        datatype = self._datatype and self._parse_datatype()
        if self._column is not None:
            return PlanFieldRef.of_column(self._dataset, self._column, datatype)
        return PlanFieldRef.of_expression(
            self._dataset,
            Expression(
                [
                    DialectExpression(dialect, expression)
                    for dialect, expression in self._expression.items()
                ]
            ),
            datatype,
        )

    @staticmethod
    def from_plan_field_ref(
        plan_field: PlanFieldRef,
    ) -> "PlanFieldRefDTO":
        """Creates a DTO from an API field reference."""
        expressions = None
        if plan_field.expression() is not None:
            expressions = {
                dialect_expression.dialect(): dialect_expression.expression()
                for dialect_expression in plan_field.expression().dialects()
            }
        return PlanFieldRefDTO(
            _dataset=plan_field.dataset(),
            _expression=expressions,
            _column=plan_field.column(),
            _datatype=(
                None if plan_field.datatype() is None else plan_field.datatype().name
            ),
        )

    def _parse_datatype(self) -> DataType:
        try:
            return DataType[self._datatype.upper()]
        except KeyError as e:
            raise IllegalArgumentException(
                f"Unsupported Semantic Model data type '{self._datatype}'"
            ) from e
