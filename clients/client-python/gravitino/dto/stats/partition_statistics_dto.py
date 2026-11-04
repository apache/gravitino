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

from dataclasses_json import DataClassJsonMixin, config

from gravitino.api.stats.partition_statistics import PartitionStatistics
from gravitino.api.stats.statistic import Statistic
from gravitino.dto.stats.statistic_dto import StatisticDTO
from gravitino.utils.precondition import Precondition


@dataclass
class PartitionStatisticsDTO(PartitionStatistics, DataClassJsonMixin):
    """The wire representation implementing the public partition result."""

    _partition_name: str = field(metadata=config(field_name="partitionName"))
    _statistics: list[StatisticDTO] = field(metadata=config(field_name="statistics"))

    def partition_name(self) -> str:
        return self._partition_name

    def statistics(self) -> list[Statistic]:
        return list(self._statistics)

    def validate(self) -> None:
        """Reject missing required fields while permitting empty statistics."""
        Precondition.check_argument(
            isinstance(self._partition_name, str)
            and bool(self._partition_name.strip()),
            '"partitionName" must not be blank',
        )
        Precondition.check_argument(
            isinstance(self._statistics, list), '"statistics" must not be null'
        )
        for statistic in self._statistics:
            Precondition.check_argument(
                isinstance(statistic, StatisticDTO), '"statistic" must not be null'
            )
            statistic.validate()
            Precondition.check_argument(
                isinstance(statistic.reserved(), bool)
                and isinstance(statistic.modifiable(), bool),
                '"reserved" and "modifiable" must be booleans',
            )
