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

"""Definition of the Statistic interface for Apache Gravitino."""

from abc import ABC, abstractmethod
from typing import Optional

from gravitino.api.statistics.statistic_value import StatisticValue


class Statistic(ABC):
    """
    Represents a statistic for a metadata object. A statistic is a key-value pair where the key is
    the statistic name and the value is the statistic value.
    """

    CUSTOM_PREFIX = "custom."

    @abstractmethod
    def name(self) -> str:
        """
        Get the name of the statistic.

        Returns:
            The name of the statistic.
        """
        pass

    @abstractmethod
    def value(self) -> Optional[StatisticValue]:
        """
        Get the value of the statistic.

        Returns:
            The value of the statistic, or None if not set.
        """
        pass

    @abstractmethod
    def reserved(self) -> bool:
        """
        Check if the statistic is reserved (system-defined).

        Returns:
            True if the statistic is reserved, False otherwise.
        """
        pass

    @abstractmethod
    def modifiable(self) -> bool:
        """
        Check if the statistic is modifiable.

        Returns:
            True if the statistic is modifiable, False otherwise.
        """
        pass
