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

from gravitino.api.semantic.semantic_query_filter import SemanticQueryFilter
from gravitino.dto.semantic.json_serdes.semantic_json_mixin import SemanticJsonMixin
from gravitino.exceptions.base import IllegalArgumentException
from gravitino.utils.precondition import Precondition


@dataclass
class SemanticQueryFilterDTO(SemanticJsonMixin):
    """Represents one filter of a Semantic Model query."""

    _field: Optional[str] = field(default=None, metadata=config(field_name="field"))
    _operator: Optional[str] = field(
        default=None, metadata=config(field_name="operator")
    )
    _value: Optional[str] = field(default=None, metadata=config(field_name="value"))

    def validate(self) -> None:
        """Validates the filter.

        Raises:
            IllegalArgumentException: If a field is missing or the operator is unknown.
        """
        Precondition.check_string_not_empty(
            self._field, '"field" field is required and cannot be empty'
        )
        Precondition.check_string_not_empty(
            self._operator, '"operator" field is required and cannot be empty'
        )
        Precondition.check_argument(
            self._value is not None, '"value" field is required and cannot be null'
        )
        self._parse_operator()

    def to_filter(self) -> SemanticQueryFilter:
        """Converts this DTO to an API filter.

        Raises:
            IllegalArgumentException: If the filter is invalid.
        """
        self.validate()
        return SemanticQueryFilter.of(self._field, self._parse_operator(), self._value)

    @staticmethod
    def from_filter(query_filter: SemanticQueryFilter) -> "SemanticQueryFilterDTO":
        """Creates a DTO from an API filter."""
        return SemanticQueryFilterDTO(
            _field=query_filter.field(),
            _operator=query_filter.operator().value.lower(),
            _value=query_filter.value(),
        )

    def _parse_operator(self) -> SemanticQueryFilter.Operator:
        try:
            return SemanticQueryFilter.Operator[self._operator.upper()]
        except KeyError as e:
            raise IllegalArgumentException(
                f"Unsupported filter operator '{self._operator}'"
            ) from e
