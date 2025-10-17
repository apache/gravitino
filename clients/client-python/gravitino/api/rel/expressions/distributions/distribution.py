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
from abc import abstractmethod
from typing import List

from gravitino.api.rel.expressions.distributions.strategy import Strategy
from gravitino.api.rel.expressions.expression import Expression


class Distribution(Expression):
    """
    An interface that defines how data is distributed across partitions.
    """

    @abstractmethod
    def strategy(self) -> Strategy:
        """Return the distribution strategy name."""

    @abstractmethod
    def number(self) -> int:
        """Return The number of buckets/distribution. For example, if the distribution strategy is HASH
        and the number is 10, then the data is distributed across 10 buckets."""

    @abstractmethod
    def expressions(self) -> List[Expression]:
        """Return The expressions passed to the distribution function."""

    def children(self) -> List[Expression]:
        """
        Returns the child expressions.
        """
        return self.expressions()

    def equals(self, other: "Distribution") -> bool:
        """
        Indicates whether some other object is "equal to" this one.

        Args:
            other (Distribution): The reference distribution object with which to compare.

        Returns:
            bool: True if this object is the same as the other; False otherwise.
        """
        if other is None:
            return False

        return (
            self.strategy() == other.strategy()
            and self.number() == other.number()
            and self.expressions() == other.expressions()
        )
