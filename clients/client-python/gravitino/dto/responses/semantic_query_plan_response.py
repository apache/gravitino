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
from gravitino.exceptions.base import IllegalArgumentException


@dataclass
class SemanticQueryPlanResponse(SemanticJsonMixin, BaseResponse):
    """Represents the SQL compiled from a Semantic Model query."""

    _sql: Optional[str] = field(default=None, metadata=config(field_name="sql"))
    _dialect: Optional[str] = field(default=None, metadata=config(field_name="dialect"))
    _planner: Optional[str] = field(default=None, metadata=config(field_name="planner"))
    _warnings: Optional[list[str]] = field(
        default=None, metadata=config(field_name="warnings")
    )

    def validate(self) -> None:
        """Validates the response data.

        Raises:
            IllegalArgumentException: If a required value is missing or invalid.
        """
        super().validate()
        if self._sql is None or not self._sql.strip():
            raise IllegalArgumentException("sql must not be null or empty")
        if self._dialect is None or not self._dialect.strip():
            raise IllegalArgumentException("dialect must not be null or empty")
        if self._planner is None or not self._planner.strip():
            raise IllegalArgumentException("planner must not be null or empty")
        if self._warnings is None:
            raise IllegalArgumentException("warnings must not be null")
        for index, warning in enumerate(self._warnings):
            if warning is None or not warning.strip():
                raise IllegalArgumentException(
                    f"warnings[{index}] must not be null or empty"
                )

    def to_plan(self) -> SemanticQueryPlan:
        """Converts this response to an API plan.

        Raises:
            IllegalArgumentException: If the response is invalid.
        """
        self.validate()
        return SemanticQueryPlan(
            sql=self._sql,
            dialect=self._dialect,
            planner=self._planner,
            warnings=list(self._warnings),
        )
