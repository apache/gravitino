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
from collections.abc import Mapping
from typing import Generic, TypeVar, Union

from gravitino.api.rel.types.type import Type

AtomicStatisticValue = Union[bool, int, float, str]


_StatisticValueT = TypeVar(
    "_StatisticValueT",
    bound=Union[
        AtomicStatisticValue,
        list["StatisticValue"],
        Mapping[str, "StatisticValue"],
    ],
)


class StatisticValue(Generic[_StatisticValueT], ABC):
    """An interface representing a statistic value.

    Type Parameters:
        _StatisticValueT: The type of the statistic value.
    """

    @abstractmethod
    def value(self) -> _StatisticValueT:
        """Returns the value of the statistic.

        Returns:
            _StatisticValueT: the value of the statistic
        """

    @abstractmethod
    def data_type(self) -> Type:
        """Returns the data type of the statistic value.

        Returns:
            Type: the data type of the statistic value
        """
