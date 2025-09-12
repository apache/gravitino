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

"""REST request for updating statistics."""

from typing import Dict

from gravitino.dto.stats.statistic_value_dto import StatisticValueDTO
from gravitino.rest.rest_message import RESTRequest


class StatisticsUpdateRequest(RESTRequest):
    """Request to update statistics."""

    _statistics: Dict[str, StatisticValueDTO]

    def __init__(self, statistics: Dict[str, StatisticValueDTO] = None):
        self._statistics = statistics or {}

    def validate(self):
        assert (
            self._statistics is not None and len(self._statistics) > 0
        ), "Statistics must not be None or empty"

    def to_json(self) -> dict:  # pylint: disable=arguments-differ
        """Convert to JSON representation."""
        return {
            "statistics": {k: v.to_json() for k, v in self._statistics.items()},
        }

    @classmethod
    def from_json(
        cls, json_dict: dict
    ) -> (
        "StatisticsUpdateRequest"
    ):  # pylint: disable=arguments-differ,arguments-renamed
        """Create from JSON representation."""
        statistics_dict = json_dict.get("statistics", {})
        statistics = {
            k: StatisticValueDTO.from_json(v) for k, v in statistics_dict.items()
        }
        return cls(statistics=statistics)
