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

from typing import List

from gravitino.api.expressions.distributions.strategy import Strategy
from gravitino.api.expressions.expression import Expression


class Distribution(Expression):
    """An interface that defines how data is distributed across partitions."""

    def strategy(self) -> Strategy:
        """
        Return the distribution strategy name.
        """
        raise NotImplementedError

    def number(self) -> int:
        """
             Return the number of buckets/distribution.
             For example, if the distribution strategy is HASH
        *    and the number is 10, then the data is distributed across 10 buckets.
        """
        raise NotImplementedError

    def expressions(self) -> List[Expression]:
        """Return the expressions passed to the distribution function."""
        raise NotImplementedError

    def children(self) -> List[Expression]:
        return self.expressions()

    def equals(self, distribution) -> bool:
        """
         Indicates whether some other object is "equal to" this one.

        Args:
            distribution The reference distribution object with which to compare.

        Returns:
            Returns true if this object is the same as the obj argument; false otherwise.
        """
        if distribution is None:
            return False

        return (
            self.strategy() == distribution.strategy()
            and self.number() == distribution.number()
            and self.expressions() == distribution.expressions()
        )
