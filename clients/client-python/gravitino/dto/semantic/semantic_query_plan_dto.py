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

from gravitino.api.semantic.semantic_query_plan import SemanticQueryPlan
from gravitino.dto.semantic.json_serdes.semantic_json_mixin import SemanticJsonMixin
from gravitino.dto.semantic.plan_filter_dto import PlanFilterDTO
from gravitino.dto.semantic.plan_join_dto import PlanJoinDTO
from gravitino.dto.semantic.plan_selection_dto import PlanSelectionDTO
from gravitino.dto.semantic.plan_source_dto import PlanSourceDTO
from gravitino.dto.semantic.semantic_dto_utils import is_none
from gravitino.name_identifier import NameIdentifier
from gravitino.utils.precondition import Precondition


@dataclass
class SemanticQueryPlanDTO(  # pylint: disable=too-many-instance-attributes
    SemanticJsonMixin
):
    """Represents the dialect-neutral logical plan returned by the server."""

    _semantic_model: Optional[list[str]] = field(
        default=None, metadata=config(field_name="semanticModel")
    )
    _semantic_model_version: Optional[int] = field(
        default=None, metadata=config(field_name="semanticModelVersion")
    )
    _sources: Optional[list[PlanSourceDTO]] = field(
        default=None, metadata=config(field_name="sources")
    )
    _joins: Optional[list[PlanJoinDTO]] = field(
        default=None, metadata=config(field_name="joins")
    )
    _selections: Optional[list[PlanSelectionDTO]] = field(
        default=None, metadata=config(field_name="selections")
    )
    _filters: Optional[list[PlanFilterDTO]] = field(
        default=None, metadata=config(field_name="filters")
    )
    _group_by: Optional[list[str]] = field(
        default=None, metadata=config(field_name="groupBy")
    )
    _limit: Optional[int] = field(
        default=None, metadata=config(field_name="limit", exclude=is_none)
    )
    _warnings: Optional[list[str]] = field(
        default=None, metadata=config(field_name="warnings")
    )

    def validate(self) -> None:
        """Validates the plan.

        Raises:
            IllegalArgumentException: If the plan violates the wire contract.
        """
        Precondition.check_argument(
            self._semantic_model is not None and len(self._semantic_model) > 0,
            '"semanticModel" field is required and cannot be empty',
        )
        for index, level in enumerate(self._semantic_model):
            Precondition.check_string_not_empty(
                level, f'"semanticModel"[{index}] field cannot be null or empty'
            )
        Precondition.check_argument(
            isinstance(self._semantic_model_version, int)
            and not isinstance(self._semantic_model_version, bool)
            and self._semantic_model_version > 0,
            '"semanticModelVersion" must be positive',
        )
        Precondition.check_argument(
            self._sources is not None and len(self._sources) > 0,
            '"sources" field is required and cannot be empty',
        )
        for index, source in enumerate(self._sources):
            Precondition.check_argument(
                source is not None, f'"sources"[{index}] field is required'
            )
            source.validate()
        Precondition.check_argument(
            self._joins is not None, '"joins" field is required'
        )
        for index, join in enumerate(self._joins):
            Precondition.check_argument(
                join is not None, f'"joins"[{index}] field is required'
            )
            join.validate()
        Precondition.check_argument(
            self._selections is not None and len(self._selections) > 0,
            '"selections" field is required and cannot be empty',
        )
        for index, selection in enumerate(self._selections):
            Precondition.check_argument(
                selection is not None, f'"selections"[{index}] field is required'
            )
            selection.validate()
        Precondition.check_argument(
            self._filters is not None, '"filters" field is required'
        )
        for index, query_filter in enumerate(self._filters):
            Precondition.check_argument(
                query_filter is not None, f'"filters"[{index}] field is required'
            )
            query_filter.validate()
        Precondition.check_argument(
            self._group_by is not None, '"groupBy" field is required'
        )
        for index, name in enumerate(self._group_by):
            Precondition.check_string_not_empty(
                name, f'"groupBy"[{index}] field cannot be null or empty'
            )
        Precondition.check_argument(
            self._limit is None
            or (
                isinstance(self._limit, int)
                and not isinstance(self._limit, bool)
                and self._limit >= 0
            ),
            '"limit" must be null or non-negative',
        )
        Precondition.check_argument(
            self._warnings is not None, '"warnings" field is required'
        )
        for index, warning in enumerate(self._warnings):
            Precondition.check_argument(
                isinstance(warning, str), f'"warnings"[{index}] cannot be null'
            )

    def to_plan(self) -> SemanticQueryPlan:
        """Converts this DTO to an API plan.

        Raises:
            IllegalArgumentException: If the plan is invalid.
        """
        self.validate()
        return SemanticQueryPlan(
            semantic_model=NameIdentifier.of(*self._semantic_model),
            semantic_model_version=self._semantic_model_version,
            sources=[source.to_plan_source() for source in self._sources],
            joins=[join.to_plan_join() for join in self._joins],
            selections=[
                selection.to_plan_selection() for selection in self._selections
            ],
            filters=[query_filter.to_plan_filter() for query_filter in self._filters],
            group_by=list(self._group_by),
            limit=self._limit,
            warnings=list(self._warnings),
        )

    @staticmethod
    def from_plan(plan: SemanticQueryPlan) -> "SemanticQueryPlanDTO":
        """Creates a DTO from an API plan."""
        levels = list(plan.semantic_model().namespace().levels())
        levels.append(plan.semantic_model().name())
        return SemanticQueryPlanDTO(
            _semantic_model=levels,
            _semantic_model_version=plan.semantic_model_version(),
            _sources=[
                PlanSourceDTO.from_plan_source(source) for source in plan.sources()
            ],
            _joins=[PlanJoinDTO.from_plan_join(join) for join in plan.joins()],
            _selections=[
                PlanSelectionDTO.from_plan_selection(selection)
                for selection in plan.selections()
            ],
            _filters=[
                PlanFilterDTO.from_plan_filter(query_filter)
                for query_filter in plan.filters()
            ],
            _group_by=plan.group_by(),
            _limit=plan.limit(),
            _warnings=plan.warnings(),
        )
