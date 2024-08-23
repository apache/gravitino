"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
"""

from abc import ABC, abstractmethod
from typing import List

from .strategy import Strategy
from ..expression import Expression

class Distribution(Expression, ABC):
    """
    An interface that defines how data is distributed across partitions.
    This can include strategies like hash, range, or list distributions.
    """

    @abstractmethod
    def strategy(self) -> Strategy:
        """Return the distribution strategy name."""
        pass

    @abstractmethod
    def number(self) -> int:
        """
        Return the number of buckets or distributions.
        For example, if the strategy is HASH and the number is 10, then the data is distributed across 10 buckets.
        """
        pass

    @abstractmethod
    def expressions(self) -> List[Expression]:
        """Return the expressions passed to the distribution function."""
        pass

    def children(self) -> List[Expression]:
        """Return a list containing the expressions that determine the distribution."""
        return self.expressions()

    def equals(self, distribution: 'Distribution') -> bool:
        """
        Check if this Distribution is equal to another Distribution.
        """
        if distribution is None:
            return False
        return (self.strategy() == distribution.strategy() and
                self.number() == distribution.number() and
                self.expressions() == distribution.expressions())
