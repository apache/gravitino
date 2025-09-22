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
from typing import Optional

from gravitino.api.auditable import Auditable
from gravitino.api.stats.statistic_value import StatisticValue


class Statistic(Auditable, ABC):
    """Statistic interface represents a statistic that can be associated with a metadata object.

    It can be used to store various types of statistics, for example, table statistics, partition
    statistics, fileset statistics, etc.
    """

    CUSTOM_PREFIX: str = "custom-"
    """The prefix for custom statistics. Custom statistics are user-defined statistics."""

    @abstractmethod
    def name(self) -> str:
        """Get the name of the statistic.

        Returns:
            str: the name of the statistic
        """

    @abstractmethod
    def value(self) -> Optional[StatisticValue]:
        """Get the value of the statistic.

        he value is optional. If the statistic is not set, the value will be empty.

        Returns:
            StatisticValue:
                An optional containing the value of the statistic if it is set, otherwise empty.
        """

    @abstractmethod
    def reserved(self) -> bool:
        """The statistic is predefined by Gravitino if the value is true.

        The statistic is defined by users if the value is false. For example, the statistic
        "row_count" is a reserved statistic. A custom statistic name must start with "custom."
        prefix to avoid name conflict with reserved statistics. Because Gravitino may add more
        reserved statistics in the future.

        Returns:
            bool: The type of the statistic. `True` if the statistic is reserved, `False` otherwise
        """

    @abstractmethod
    def modifiable(self) -> bool:
        """Whether the statistic is modifiable.

        Returns:
            bool: If the statistic is modifiable, return `True`, otherwise `False`.
        """
