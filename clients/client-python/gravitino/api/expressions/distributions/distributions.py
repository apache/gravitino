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

from gravitino.api.expressions.distributions.strategy import Strategy
from gravitino.api.expressions.distributions.distribution import Distribution
from gravitino.api.expressions.expression import Expression
from gravitino.api.expressions.named_reference import NamedReference


class DistributionImpl(Distribution):
    def __init__(self, strategy: str, number: int, expressions: list[Expression]):
        self._strategy = strategy
        self._number = number
        self._expressions = expressions

    def strategy(self) -> str:
        return self._strategy

    def number(self) -> int:
        return self._number

    def expressions(self) -> list[Expression]:
        return self._expressions

    def __str__(self) -> str:
        return f"DistributionImpl(strategy={self._strategy}, number={self._number}, expressions={self._expressions})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, DistributionImpl):
            return False
        return (
            self._strategy == other._strategy
            and self._number == other._number
            and self._expressions == other._expressions
        )

    def __hash__(self) -> int:
        return hash((self._strategy, self._number, tuple(self._expressions)))


class Distributions:
    NONE = DistributionImpl(Strategy.NONE, 0, [Expression.EMPTY_EXPRESSION])
    HASH = DistributionImpl(Strategy.HASH, 0, [Expression.EMPTY_EXPRESSION])
    RANGE = DistributionImpl(Strategy.RANGE, 0, [Expression.EMPTY_EXPRESSION])

    @staticmethod
    def even(number: int, *expressions: Expression) -> Distribution:
        return DistributionImpl(Strategy.EVEN, number, list(expressions))

    @staticmethod
    def hash(number: int, *expressions: Expression) -> Distribution:
        return DistributionImpl(Strategy.HASH, number, list(expressions))

    @staticmethod
    def of(strategy: str, number: int, *expressions: Expression) -> Distribution:
        return DistributionImpl(strategy, number, list(expressions))

    @staticmethod
    def fields(strategy: str, number: int, *field_names: list[str]) -> Distribution:
        expressions = [
            NamedReference.field(name)
            for name_list in field_names
            for name in name_list
        ]
        return Distributions.of(strategy, number, *expressions)
