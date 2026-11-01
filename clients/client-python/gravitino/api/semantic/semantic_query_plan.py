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

from gravitino.api.semantic.plan_filter import PlanFilter
from gravitino.api.semantic.plan_join import PlanJoin
from gravitino.api.semantic.plan_selection import PlanSelection
from gravitino.api.semantic.plan_source import PlanSource
from gravitino.name_identifier import NameIdentifier
from gravitino.utils.precondition import Precondition


class SemanticQueryPlan:  # pylint: disable=too-many-instance-attributes
    """A dialect-neutral logical plan resolved from a `SemanticQuery`.

    The plan is a single relational block. It names the physical sources and
    join structure, but it never contains SQL text, alias names, quoting,
    literal syntax or a dialect choice. A consumer turns it into an
    engine-specific plan; resolving a plan is not authorization to execute it.
    """

    # pylint: disable=too-many-locals
    def __init__(
        self,
        semantic_model: NameIdentifier,
        semantic_model_version: int,
        sources: list[PlanSource],
        joins: list[PlanJoin],
        selections: list[PlanSelection],
        filters: list[PlanFilter],
        group_by: list[str],
        limit: Optional[int] = None,
        warnings: Optional[list[str]] = None,
    ):
        warnings = [] if warnings is None else warnings
        Precondition.check_argument(
            semantic_model is not None, "semanticModel must not be null"
        )
        Precondition.check_argument(
            isinstance(semantic_model_version, int)
            and not isinstance(semantic_model_version, bool)
            and semantic_model_version > 0,
            "semanticModelVersion must be positive",
        )
        Precondition.check_argument(
            sources is not None and len(sources) > 0,
            "sources must not be null or empty",
        )
        Precondition.check_argument(
            selections is not None and len(selections) > 0,
            "selections must not be null or empty",
        )
        Precondition.check_argument(joins is not None, "joins must not be null")
        Precondition.check_argument(filters is not None, "filters must not be null")
        Precondition.check_argument(group_by is not None, "groupBy must not be null")
        Precondition.check_argument(
            limit is None
            or (isinstance(limit, int) and not isinstance(limit, bool) and limit >= 0),
            "limit must be null or non-negative",
        )

        source_datasets = []
        for index, source in enumerate(sources):
            Precondition.check_argument(
                isinstance(source, PlanSource), f"sources[{index}] must not be null"
            )
            Precondition.check_argument(
                source.dataset() not in source_datasets,
                f"sources must not contain duplicate dataset: {source.dataset()}",
            )
            source_datasets.append(source.dataset())

        selection_names = set()
        expected_group_by = []
        for index, selection in enumerate(selections):
            Precondition.check_argument(
                isinstance(selection, PlanSelection),
                f"selections[{index}] must not be null",
            )
            Precondition.check_argument(
                selection.name() not in selection_names,
                f"selection names must be unique: {selection.name()}",
            )
            selection_names.add(selection.name())
            Precondition.check_argument(
                selection.field().dataset() in source_datasets,
                f"selection {selection.name()} references dataset "
                f"{selection.field().dataset()}, which is not in sources",
            )
            if not selection.aggregate():
                expected_group_by.append(selection.name())
        Precondition.check_argument(
            expected_group_by == list(group_by),
            "groupBy must equal the non-aggregate selection names in selection order",
        )

        source_indexes = {
            source.dataset(): index for index, source in enumerate(sources)
        }
        for index, join in enumerate(joins):
            Precondition.check_argument(
                isinstance(join, PlanJoin), f"joins[{index}] must not be null"
            )
            from_index = source_indexes.get(join.from_dataset())
            to_index = source_indexes.get(join.to_dataset())
            Precondition.check_argument(
                from_index is not None and to_index is not None,
                f"join {index} must reference datasets declared in sources",
            )
            Precondition.check_argument(
                from_index < to_index,
                f"join {index} must add a dataset that appears later in sources",
            )

        for index, query_filter in enumerate(filters):
            Precondition.check_argument(
                isinstance(query_filter, PlanFilter),
                f"filters[{index}] must not be null",
            )
            Precondition.check_argument(
                query_filter.field().dataset() in source_datasets,
                f"filter {index} references dataset "
                f"{query_filter.field().dataset()}, which is not in sources",
            )

        for index, warning in enumerate(warnings):
            Precondition.check_argument(
                isinstance(warning, str),
                f"warnings[{index}] must not be null",
            )

        self._semantic_model = semantic_model
        self._semantic_model_version = semantic_model_version
        self._sources = tuple(sources)
        self._joins = tuple(joins)
        self._selections = tuple(selections)
        self._filters = tuple(filters)
        self._group_by = tuple(group_by)
        self._limit = limit
        self._warnings = tuple(warnings)

    def semantic_model(self) -> NameIdentifier:
        """Returns the complete identifier of the Semantic Model."""
        return self._semantic_model

    def semantic_model_version(self) -> int:
        """Returns the committed version the plan was resolved against."""
        return self._semantic_model_version

    def sources(self) -> list[PlanSource]:
        """Returns the datasets the query needs, in join order."""
        return list(self._sources)

    def joins(self) -> list[PlanJoin]:
        """Returns the joins that connect the sources."""
        return list(self._joins)

    def selections(self) -> list[PlanSelection]:
        """Returns the selected metrics then dimensions, in request order."""
        return list(self._selections)

    def filters(self) -> list[PlanFilter]:
        """Returns the filters, ANDed together in request order."""
        return list(self._filters)

    def group_by(self) -> list[str]:
        """Returns the non-aggregate selection names, in selection order."""
        return list(self._group_by)

    def limit(self) -> Optional[int]:
        """Returns the row limit, or `None` for no limit."""
        return self._limit

    def warnings(self) -> list[str]:
        """Returns non-fatal warnings raised while resolving."""
        return list(self._warnings)

    def __eq__(self, other) -> bool:
        if not isinstance(other, SemanticQueryPlan):
            return False
        return (
            self._semantic_model == other.semantic_model()
            and self._semantic_model_version == other.semantic_model_version()
            and self._sources == tuple(other.sources())
            and self._joins == tuple(other.joins())
            and self._selections == tuple(other.selections())
            and self._filters == tuple(other.filters())
            and self._group_by == tuple(other.group_by())
            and self._limit == other.limit()
            and self._warnings == tuple(other.warnings())
        )

    def __hash__(self) -> int:
        return hash(
            (
                self._semantic_model,
                self._semantic_model_version,
                self._sources,
                self._joins,
                self._selections,
                self._filters,
                self._group_by,
                self._limit,
                self._warnings,
            )
        )

    def __repr__(self) -> str:
        return (
            f"SemanticQueryPlan(semanticModel={self._semantic_model!r}, "
            f"semanticModelVersion={self._semantic_model_version}, "
            f"sources={list(self._sources)!r}, joins={list(self._joins)!r}, "
            f"selections={list(self._selections)!r}, "
            f"filters={list(self._filters)!r}, groupBy={list(self._group_by)!r}, "
            f"limit={self._limit!r}, warnings={list(self._warnings)!r})"
        )
