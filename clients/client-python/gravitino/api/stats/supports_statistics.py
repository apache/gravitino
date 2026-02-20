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

from gravitino.api.stats.statistic import Statistic
from gravitino.api.stats.statistic_value import StatisticValue


class SupportsStatistics(ABC):
    """SupportsStatistics provides methods to list and update statistics.

    A table, a partition or a fileset can implement this interface to manage its statistics.
    """

    @abstractmethod
    def list_statistics(self) -> list[Statistic]:
        """Lists all statistics.

        Returns:
            list[Statistic]: a list of statistics
        """

    @abstractmethod
    def update_statistics(self, statistics: dict[str, StatisticValue]) -> None:
        """Updates statistics with the provided values.

        If the statistic exists, it will be updated with the new value. If the statistic does
        not exist, it will be created. If the statistic is unmodifiable, it will throw an
        `UnmodifiableStatisticException`. If the statistic name is illegal, it will throw an
        `IllegalStatisticNameException`.

        Args:
            statistics (dict[str, StatisticValue]):
                a map of statistic names to their values

        Raises:
            IllegalStatisticNameException: If the statistic name is illegal
            UnmodifiableStatisticException: If the statistic is unmodifiable
        """

    @abstractmethod
    def drop_statistics(self, statistics: list[str]) -> bool:
        """Drop statistics by their names.

        If the statistic is unmodifiable, it will throw an `UnmodifiableStatisticException`.

        Args:
            statistics (list[str]): a list of statistic names to be dropped

        Returns:
            bool:
                `True` if the statistics were successfully dropped,
                `False` if no statistics were dropped

        Raises:
            UnmodifiableStatisticException:
                if any of the statistics to be dropped are unmodifiable
        """
