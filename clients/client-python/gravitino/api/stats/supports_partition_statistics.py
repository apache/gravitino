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

from abc import ABC, abstractmethod

from gravitino.api.stats.partition_range import PartitionRange
from gravitino.api.stats.partition_statistics import PartitionStatistics
from gravitino.api.stats.partition_statistics_drop import PartitionStatisticsDrop
from gravitino.api.stats.partition_statistics_update import PartitionStatisticsUpdate


class SupportsPartitionStatistics(ABC):
    """Access partition statistics stored by Gravitino.

    Physical partition objects need not exist. Supported operations and batch
    limits depend on the server and its configured statistics storage backend.
    """

    @abstractmethod
    def list_partition_statistics(
        self, partition_range: PartitionRange
    ) -> list[PartitionStatistics]:
        """List partition statistics within the supplied open or closed bounds."""

    @abstractmethod
    def update_partition_statistics(
        self, updates: list[PartitionStatisticsUpdate]
    ) -> None:
        """Create or overwrite statistics with one batch request.

        Invalid names, unmodifiable statistics, missing tables and unsupported
        operations raise the existing statistics exceptions. Writes are not
        automatically retried.
        """

    @abstractmethod
    def drop_partition_statistics(self, drops: list[PartitionStatisticsDrop]) -> bool:
        """Drop the named statistics and return the server's dropped flag.

        This operation does not delete physical partitions. Storage semantics
        and failures follow the server; the client does not retry the batch.
        """
