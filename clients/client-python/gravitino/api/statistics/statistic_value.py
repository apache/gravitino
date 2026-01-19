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

"""Defines the base interface for statistic values in Gravitino."""

from abc import ABC, abstractmethod
from typing import TypeVar, Generic


T = TypeVar("T")


class StatisticValue(ABC, Generic[T]):
    """
    Represents a statistic value. A statistic value is a value that can be used to describe a
    statistic. It can be a boolean, long, double, string, list, or object.
    """

    class Type:
        """The type of the statistic value."""

        BOOLEAN = "boolean"
        LONG = "long"
        DOUBLE = "double"
        STRING = "string"
        LIST = "list"
        OBJECT = "object"

    @abstractmethod
    def value(self) -> T:
        """
        Get the value of the statistic.

        Returns:
            The value of the statistic.
        """
        pass

    @abstractmethod
    def data_type(self) -> str:
        """
        Get the data type of the statistic value.

        Returns:
            The data type of the statistic value.
        """
        pass
