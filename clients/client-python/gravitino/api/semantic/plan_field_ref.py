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

from gravitino.api.semantic.data_type import DataType
from gravitino.api.semantic.expression import Expression
from gravitino.utils.precondition import Precondition


class PlanFieldRef:
    """A reference to a field of a dataset in a `SemanticQueryPlan`.

    Exactly one of `expression` and `column` is set. A declared field carries
    its per-dialect expression; a physical column that the model does not expose
    carries only its name.
    """

    def __init__(
        self,
        dataset: str,
        expression: Optional[Expression],
        column: Optional[str],
        datatype: Optional[DataType],
    ):
        Precondition.check_string_not_empty(
            dataset, "dataset must not be null or blank"
        )
        Precondition.check_argument(
            (expression is None) != (column is None),
            "exactly one of expression and column must be set",
        )
        Precondition.check_argument(
            column is None or column.strip() != "",
            "column must not be blank when present",
        )
        self._dataset = dataset
        self._expression = expression
        self._column = column
        self._datatype = datatype

    @staticmethod
    def of_expression(
        dataset: str, expression: Expression, datatype: Optional[DataType]
    ) -> "PlanFieldRef":
        """Create a reference to a declared field."""
        return PlanFieldRef(dataset, expression, None, datatype)

    @staticmethod
    def of_column(
        dataset: str, column: str, datatype: Optional[DataType]
    ) -> "PlanFieldRef":
        """Create a reference to a physical column not exposed as a declared field."""
        return PlanFieldRef(dataset, None, column, datatype)

    def dataset(self) -> str:
        """Returns the dataset that provides the field."""
        return self._dataset

    def expression(self) -> Optional[Expression]:
        """Returns the declared field expression, or `None` for a physical column."""
        return self._expression

    def column(self) -> Optional[str]:
        """Returns the physical column name, or `None` for a declared field."""
        return self._column

    def datatype(self) -> Optional[DataType]:
        """Returns the logical data type, or `None` if the model declares none."""
        return self._datatype

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, PlanFieldRef):
            return False
        return (
            self._dataset == other.dataset()
            and self._expression == other.expression()
            and self._column == other.column()
            and self._datatype == other.datatype()
        )

    def __hash__(self) -> int:
        return hash((self._dataset, self._expression, self._column, self._datatype))

    def __repr__(self) -> str:
        return (
            f"PlanFieldRef(dataset={self._dataset!r}, "
            f"expression={self._expression!r}, column={self._column!r}, "
            f"datatype={self._datatype})"
        )
