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

"""REST response containing a list of statistics."""

import json
from typing import List

from gravitino.dto.responses.base_response import BaseResponse
from gravitino.dto.stats.statistic_dto import StatisticDTO


class StatisticListResponse(BaseResponse):
    """Response containing a list of statistics."""

    _statistics: List[StatisticDTO]

    def __init__(self, statistics: List[StatisticDTO] = None):
        super().__init__(0)
        self._statistics = statistics or []

    def statistics(self) -> List[StatisticDTO]:
        """Get the list of statistics."""
        return self._statistics

    def validate(self):
        """Validate the response."""
        super().validate()
        assert self._statistics is not None, "Statistics must not be None"

    @classmethod
    def from_json(
        cls, s, *, infer_missing=False, **kwargs
    ):  # pylint: disable=arguments-differ,unused-argument
        """Create from JSON representation."""
        # Handle both string and dict input
        if isinstance(s, str):
            json_dict = json.loads(s)
        else:
            json_dict = s

        statistics = []
        if "statistics" in json_dict:
            statistics = [
                StatisticDTO.from_json(stat) for stat in json_dict.get("statistics", [])
            ]

        resp = cls(statistics=statistics)
        resp._code = json_dict.get("code", 0)
        return resp

    def to_json(self) -> dict:  # pylint: disable=arguments-differ
        """Convert to JSON representation."""
        return {
            "code": self._code,
            "statistics": [stat.to_json() for stat in self._statistics],
        }
