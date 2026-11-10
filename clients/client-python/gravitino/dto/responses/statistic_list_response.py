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

from dataclasses_json import config

from gravitino.dto.responses.base_response import BaseResponse
from gravitino.dto.stats.statistic_dto import StatisticDTO
from gravitino.utils.precondition import Precondition


@dataclass
class StatisticListResponse(BaseResponse):
    """Represents a response containing a list of statistics."""

    _statistics: list[StatisticDTO] = field(metadata=config(field_name="statistics"))

    @property
    def statistics(self) -> list[StatisticDTO]:
        return self._statistics

    def validate(self) -> None:
        Precondition.check_argument(
            self._statistics is not None, '"statistics" must not be null'
        )
        for statistic in self._statistics:
            Precondition.check_argument(
                statistic is not None, '"statistic" must not be null'
            )
            statistic.validate()
