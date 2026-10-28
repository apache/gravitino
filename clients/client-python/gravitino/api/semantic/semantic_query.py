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

from gravitino.api.semantic.semantic_query_filter import SemanticQueryFilter
from gravitino.utils.precondition import Precondition


class SemanticQuery:
    """An immutable semantic query that a query planner compiles to SQL for an engine.

    A query selects metrics and dimensions and may restrict them with filters.
    The dialect is a non-empty identifier chosen by the caller; Gravitino
    preserves it without implicit conversion so consumers can use engines and
    custom dialect names the server does not know about.
    """

    def __init__(
        self,
        dialect: str,
        metrics: Optional[list[str]] = None,
        dimensions: Optional[list[str]] = None,
        filters: Optional[list[SemanticQueryFilter]] = None,
    ):
        Precondition.check_string_not_empty(
            dialect, "dialect must not be null or empty"
        )
        metrics = [] if metrics is None else metrics
        dimensions = [] if dimensions is None else dimensions
        filters = [] if filters is None else filters
        self._check_names("metrics", metrics)
        self._check_names("dimensions", dimensions)
        for index, query_filter in enumerate(filters):
            Precondition.check_argument(
                isinstance(query_filter, SemanticQueryFilter),
                f"filters[{index}] must not be null",
            )
        Precondition.check_argument(
            len(metrics) > 0 or len(dimensions) > 0,
            "a semantic query must select at least one metric or dimension",
        )
        self._metrics = tuple(metrics)
        self._dimensions = tuple(dimensions)
        self._filters = tuple(filters)
        self._dialect = dialect

    def metrics(self) -> list[str]:
        """Returns the selected metric names."""
        return list(self._metrics)

    def dimensions(self) -> list[str]:
        """Returns the selected dimension names."""
        return list(self._dimensions)

    def filters(self) -> list[SemanticQueryFilter]:
        """Returns the filters applied to the query."""
        return list(self._filters)

    def dialect(self) -> str:
        """Returns the target engine dialect."""
        return self._dialect

    def __eq__(self, other) -> bool:
        if not isinstance(other, SemanticQuery):
            return False
        return (
            self._metrics == other._metrics
            and self._dimensions == other._dimensions
            and self._filters == other._filters
            and self._dialect == other._dialect
        )

    def __hash__(self) -> int:
        return hash((self._metrics, self._dimensions, self._filters, self._dialect))

    def __repr__(self) -> str:
        return (
            f"SemanticQuery(metrics={list(self._metrics)!r}, "
            f"dimensions={list(self._dimensions)!r}, "
            f"filters={list(self._filters)!r}, dialect={self._dialect!r})"
        )

    @staticmethod
    def _check_names(name: str, values: list[str]) -> None:
        for index, value in enumerate(values):
            Precondition.check_string_not_empty(
                value, f"{name}[{index}] must not be null or empty"
            )
