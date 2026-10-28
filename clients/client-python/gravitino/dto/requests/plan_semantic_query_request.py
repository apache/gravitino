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

from gravitino.api.semantic.semantic_query import SemanticQuery
from gravitino.dto.semantic.json_serdes.semantic_json_mixin import SemanticJsonMixin
from gravitino.dto.semantic.semantic_query_filter_dto import SemanticQueryFilterDTO
from gravitino.rest.rest_message import RESTRequest
from gravitino.utils.precondition import Precondition


@dataclass
class PlanSemanticQueryRequest(SemanticJsonMixin, RESTRequest):
    """Represents a request to plan a Semantic Model query without executing it."""

    _metrics: Optional[list[str]] = field(
        default=None, metadata=config(field_name="metrics")
    )
    _dimensions: Optional[list[str]] = field(
        default=None, metadata=config(field_name="dimensions")
    )
    _filters: Optional[list[SemanticQueryFilterDTO]] = field(
        default=None, metadata=config(field_name="filters")
    )
    _dialect: Optional[str] = field(default=None, metadata=config(field_name="dialect"))

    def validate(self) -> None:
        """Validates the request.

        Raises:
            IllegalArgumentException: If the request is incomplete or invalid.
        """
        Precondition.check_string_not_empty(
            self._dialect, '"dialect" field is required and cannot be empty'
        )
        if self._metrics is not None:
            for index, metric in enumerate(self._metrics):
                Precondition.check_string_not_empty(
                    metric,
                    f'"metrics"[{index}] field cannot be null or empty',
                )
        if self._dimensions is not None:
            for index, dimension in enumerate(self._dimensions):
                Precondition.check_string_not_empty(
                    dimension,
                    f'"dimensions"[{index}] field cannot be null or empty',
                )
        if self._filters is not None:
            for index, query_filter in enumerate(self._filters):
                Precondition.check_argument(
                    query_filter is not None,
                    f'"filters"[{index}] field cannot be null',
                )
                query_filter.validate()
        Precondition.check_argument(
            bool(self._metrics) or bool(self._dimensions),
            "At least one metric or dimension must be selected",
        )

    def to_query(self) -> SemanticQuery:
        """Converts this request to an API query.

        Raises:
            IllegalArgumentException: If the request is invalid.
        """
        self.validate()
        return SemanticQuery(
            dialect=self._dialect,
            metrics=list(self._metrics or []),
            dimensions=list(self._dimensions or []),
            filters=[query_filter.to_filter() for query_filter in self._filters or []],
        )

    @staticmethod
    def from_query(query: SemanticQuery) -> "PlanSemanticQueryRequest":
        """Creates a request from an API query."""
        return PlanSemanticQueryRequest(
            _metrics=query.metrics(),
            _dimensions=query.dimensions(),
            _filters=[
                SemanticQueryFilterDTO.from_filter(query_filter)
                for query_filter in query.filters()
            ],
            _dialect=query.dialect(),
        )
