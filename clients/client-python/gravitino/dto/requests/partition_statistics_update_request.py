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

from gravitino.api.stats.json_serdes.statistic_value_serdes import StatisticValueSerdes
from gravitino.api.stats.partition_statistics_update import PartitionStatisticsUpdate
from gravitino.rest.rest_message import RESTRequest
from gravitino.utils.precondition import Precondition


@dataclass
class PartitionStatisticsUpdateRequest(RESTRequest):
    """A batch of public updates mapped directly to the existing wire format."""

    _updates: list[PartitionStatisticsUpdate] = field(
        metadata=config(
            field_name="updates",
            encoder=lambda updates: [
                {
                    "partitionName": update.partition_name,
                    "statistics": {
                        name: StatisticValueSerdes.serialize(value)
                        for name, value in update.statistics.items()
                    },
                }
                for update in updates
            ],
            decoder=lambda updates: [
                PartitionStatisticsUpdate(
                    partition_name=update.get("partitionName"),
                    statistics={
                        name: StatisticValueSerdes.deserialize(value)
                        for name, value in update["statistics"].items()
                    },
                )
                for update in updates
            ],
        )
    )

    def validate(self) -> None:
        """Delegate item validation to the public input objects."""
        Precondition.check_argument(
            isinstance(self._updates, list) and len(self._updates) > 0,
            '"updates" must not be null or empty',
        )
        for update in self._updates:
            Precondition.check_argument(
                isinstance(update, PartitionStatisticsUpdate),
                "Each update must be a PartitionStatisticsUpdate",
            )
            update.validate()
