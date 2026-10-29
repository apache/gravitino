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
from gravitino.dto.stats.partition_statistics_dto import PartitionStatisticsDTO
from gravitino.utils.precondition import Precondition


@dataclass
class PartitionStatisticsListResponse(BaseResponse):
    """The response containing zero or more partition statistics results."""

    _partition_statistics: list[PartitionStatisticsDTO] = field(
        metadata=config(field_name="partitionStatistics")
    )

    @property
    def partition_statistics(self) -> list[PartitionStatisticsDTO]:
        """Return the deserialized partition results."""
        return self._partition_statistics

    def validate(self) -> None:
        """Validate the response envelope and nested required fields."""
        Precondition.check_argument(
            isinstance(self._code, int), '"code" must be an integer'
        )
        super().validate()
        Precondition.check_argument(
            isinstance(self._partition_statistics, list),
            '"partitionStatistics" must not be null',
        )
        for partition in self._partition_statistics:
            Precondition.check_argument(
                isinstance(partition, PartitionStatisticsDTO),
                "Each partition statistic must not be null",
            )
            partition.validate()
