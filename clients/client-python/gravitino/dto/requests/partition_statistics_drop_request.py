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

from gravitino.api.stats.partition_statistics_drop import PartitionStatisticsDrop
from gravitino.rest.rest_message import RESTRequest
from gravitino.utils.precondition import Precondition


@dataclass
class PartitionStatisticsDropRequest(RESTRequest):
    """A batch deleting named statistics from caller-supplied partitions."""

    _drops: list[PartitionStatisticsDrop] = field(
        metadata=config(
            field_name="drops",
            encoder=lambda drops: [
                {
                    "partitionName": drop.partition_name,
                    "statisticNames": drop.statistic_names,
                }
                for drop in drops
            ],
            decoder=lambda drops: [
                PartitionStatisticsDrop(
                    partition_name=drop.get("partitionName"),
                    statistic_names=drop.get("statisticNames"),
                )
                for drop in drops
            ],
        )
    )

    def validate(self) -> None:
        """Delegate item validation to the public input objects."""
        Precondition.check_argument(
            isinstance(self._drops, list) and len(self._drops) > 0,
            '"drops" must not be null or empty',
        )
        for drop in self._drops:
            Precondition.check_argument(
                isinstance(drop, PartitionStatisticsDrop),
                "Each drop must be a PartitionStatisticsDrop",
            )
            drop.validate()
