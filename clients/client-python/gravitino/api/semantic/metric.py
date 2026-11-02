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

from gravitino.api.semantic.ai_context import AIContext
from gravitino.api.semantic.custom_extension import CustomExtension
from gravitino.api.semantic.data_type import DataType
from gravitino.api.semantic.expression import Expression
from gravitino.api.semantic.semantic_utils import check_no_none_elements
from gravitino.utils.precondition import Precondition


class Metric:
    """A governed measure defined by a Semantic Model.

    Metric names are unique within a Semantic Model. Metrics may reference fields
    and datasets in the same Semantic Model, cross-model references are not
    defined by this contract.
    """

    def __init__(
        self,
        name: str,
        expression: Expression,
        description: Optional[str] = None,
        datatype: Optional[DataType] = None,
        ai_context: Optional[AIContext] = None,
        custom_extensions: Optional[list[CustomExtension]] = None,
    ):
        Precondition.check_argument(
            name is not None and name != "", "name must not be null or empty"
        )
        Precondition.check_argument(
            expression is not None, "expression must not be null"
        )
        check_no_none_elements("customExtensions", custom_extensions)

        self._name = name
        self._expression = expression
        self._description = description
        self._datatype = datatype
        self._ai_context = ai_context
        self._custom_extensions = (
            None if custom_extensions is None else list(custom_extensions)
        )

    def name(self) -> str:
        """Returns the metric name."""
        return self._name

    def expression(self) -> Expression:
        """Returns the expression that computes the metric."""
        return self._expression

    def description(self) -> Optional[str]:
        """Returns the metric description, or `None` if it is not set."""
        return self._description

    def datatype(self) -> Optional[DataType]:
        """Returns the logical data type, or `None` if it is not set."""
        return self._datatype

    def ai_context(self) -> Optional[AIContext]:
        """Returns the AI context, or `None` if it is not set."""
        return self._ai_context

    def custom_extensions(self) -> Optional[list[CustomExtension]]:
        """Returns the custom extensions, or `None` if they are not set."""
        return (
            None if self._custom_extensions is None else list(self._custom_extensions)
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Metric):
            return False
        return (
            self._name == other.name()
            and self._expression == other.expression()
            and self._description == other.description()
            and self._datatype == other.datatype()
            and self._ai_context == other.ai_context()
            and self._custom_extensions == other.custom_extensions()
        )

    def __hash__(self) -> int:
        return hash(
            (
                self._name,
                self._expression,
                self._description,
                self._datatype,
                self._ai_context,
                (
                    None
                    if self._custom_extensions is None
                    else tuple(self._custom_extensions)
                ),
            )
        )

    def __repr__(self) -> str:
        return (
            f"Metric(name={self._name!r}, expression={self._expression!r}, "
            f"description={self._description!r}, datatype={self._datatype!r}, "
            f"aiContext={self._ai_context!r}, "
            f"customExtensions={self._custom_extensions!r})"
        )
