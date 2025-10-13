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

from gravitino.api.rel.expressions.distributions.distribution import Distribution
from gravitino.api.rel.expressions.distributions.strategy import Strategy
from gravitino.api.rel.expressions.expression import Expression
from gravitino.api.rel.expressions.named_reference import NamedReference


class DistributionImpl(Distribution):
    _strategy: Strategy
    _number: int
    _expressions: List[Expression]

    def __init__(self, strategy: Strategy, number: int, expressions: List[Expression]):
        self._strategy = strategy
        self._number = number
        self._expressions = expressions

    def strategy(self) -> Strategy:
        return self._strategy

    def number(self) -> int:
        return self._number

    def expressions(self) -> List[Expression]:
        return self._expressions

    def __str__(self) -> str:
        return f"DistributionImpl(strategy={self._strategy}, number={self._number}, expressions={self._expressions})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, DistributionImpl):
            return False
        return (
            self._strategy == other.strategy()
            and self._number == other.number()
            and self._expressions == other.expressions()
        )

    def __hash__(self) -> int:
        return hash((self._strategy, self._number, tuple(self._expressions)))


class Distributions:
    NONE: Distribution = DistributionImpl(Strategy.NONE, 0, Expression.EMPTY_EXPRESSION)
    """NONE is used to indicate that there is no distribution."""
    HASH: Distribution = DistributionImpl(Strategy.HASH, 0, Expression.EMPTY_EXPRESSION)
    """List bucketing strategy hash, TODO: #1505 Separate the bucket number from the Distribution."""
    RANGE: Distribution = DistributionImpl(
        Strategy.RANGE, 0, Expression.EMPTY_EXPRESSION
    )
    """List bucketing strategy range, TODO: #1505 Separate the bucket number from the Distribution."""

    @staticmethod
    def even(number: int, *expressions: Expression) -> Distribution:
        """
        Create a distribution by evenly distributing the data across the number of buckets.

        :param number: The number of buckets.
        :param expressions: The expressions to distribute by.
        :return: The created even distribution.
        """
        return DistributionImpl(Strategy.EVEN, number, list(expressions))

    @staticmethod
    def hash(number: int, *expressions: Expression) -> Distribution:
        """
        Create a distribution by hashing the data across the number of buckets.

        :param number: The number of buckets.
        :param expressions: The expressions to distribute by.
        :return: The created hash distribution.
        """
        return DistributionImpl(Strategy.HASH, number, list(expressions))

    @staticmethod
    def of(strategy: Strategy, number: int, *expressions: Expression) -> Distribution:
        """
        Create a distribution by the given strategy.

        :param strategy: The strategy to use.
        :param number: The number of buckets.
        :param expressions: The expressions to distribute by.
        :return: The created distribution.
        """
        return DistributionImpl(strategy, number, list(expressions))

    @staticmethod
    def fields(
        strategy: Strategy, number: int, *field_names: List[str]
    ) -> Distribution:
        """
        Create a distribution on columns. Like distribute by (a) or (a, b), for complex like
        distributing by (func(a), b) or (func(a), func(b)), please use DistributionImpl.Builder to create.

        NOTE: a, b, c are column names.

        SQL syntax: distribute by hash(a, b) buckets 5
        fields(Strategy.HASH, 5, ["a"], ["b"])

        SQL syntax: distribute by hash(a, b, c) buckets 10
        fields(Strategy.HASH, 10, ["a"], ["b"], ["c"])

        SQL syntax: distribute by EVEN(a) buckets 128
        fields(Strategy.EVEN, 128, ["a"])

        :param strategy: The strategy to use.
        :param number: The number of buckets.
        :param field_names: The field names to distribute by.
        :return: The created distribution.
        """
        expressions = [NamedReference.field(name) for name in field_names]
        return Distributions.of(strategy, number, *expressions)
