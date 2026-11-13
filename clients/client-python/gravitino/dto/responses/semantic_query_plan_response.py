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
from gravitino.dto.responses.base_response import BaseResponse
from gravitino.dto.semantic.json_serdes.semantic_json_mixin import SemanticJsonMixin
from gravitino.dto.semantic.semantic_query_plan_dto import SemanticQueryPlanDTO
from gravitino.exceptions.base import IllegalArgumentException


@dataclass
class SemanticQueryPlanResponse(SemanticJsonMixin, BaseResponse):
    """Represents the dialect-neutral plan resolved for a Semantic Model query."""

    _plan: Optional[SemanticQueryPlanDTO] = field(
        default=None, metadata=config(field_name="plan")
    )

    def plan(self) -> Optional[SemanticQueryPlanDTO]:
        """Returns the plan DTO."""
        return self._plan

    def validate(self) -> None:
        """Validates the response data.

        Raises:
            IllegalArgumentException: If the plan is missing or invalid.
        """
        super().validate()
        if self._plan is None:
            raise IllegalArgumentException("plan must not be null")
        self._plan.validate()

    def to_plan(self) -> SemanticQueryPlan:
        """Converts this response to an API plan.

        Raises:
            IllegalArgumentException: If the response is invalid.
        """
        self.validate()
        return self._plan.to_plan()
