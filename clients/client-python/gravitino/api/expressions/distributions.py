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

from typing import List, Tuple
from gravitino.api.expressions.named_reference import NamedReference


# Enum equivalent in Python for Strategy
class Strategy:
    NONE = "NONE"
    HASH = "HASH"
    RANGE = "RANGE"
    EVEN = "EVEN"

    @staticmethod
    def get_by_name(name: str):
        name = name.upper()
        if name == "NONE":
            return Strategy.NONE
        if name == "HASH":
            return Strategy.HASH
        if name == "RANGE":
            return Strategy.RANGE
        if name in ("EVEN", "RANDOM"):
            return Strategy.EVEN
        raise ValueError(
            f"Invalid distribution strategy: {name}. "
            f"Valid values are: {', '.join([Strategy.NONE, Strategy.HASH, Strategy.RANGE, Strategy.EVEN])}"
        )


# Distribution interface equivalent
class Distribution:
    def strategy(self) -> str:
        raise NotImplementedError

    def number(self) -> int:
        raise NotImplementedError

    def expressions(self) -> List:
        raise NotImplementedError

    def children(self) -> List:
        return self.expressions()

    def equals(self, distribution) -> bool:
        return (
            isinstance(distribution, Distribution)
            and self.strategy() == distribution.strategy()
            and self.number() == distribution.number()
            and self.expressions() == distribution.expressions()
        )


# Implementation of Distribution
class DistributionImpl(Distribution):
    def __init__(self, strategy: str, number: int, expressions: List):
        self._strategy = strategy
        self._number = number
        self._expressions = expressions

    def strategy(self) -> str:
        return self._strategy

    def number(self) -> int:
        return self._number

    def expressions(self) -> List:
        return self._expressions

    def __str__(self):
        return f"DistributionImpl(strategy={self._strategy}, number={self._number}, expressions={self._expressions})"

    def __eq__(self, other):
        if not isinstance(other, DistributionImpl):
            return False
        return (
            self._strategy == other._strategy
            and self._number == other._number
            and self._expressions == other._expressions
        )

    def __hash__(self):
        return hash((self._strategy, self._number, tuple(self._expressions)))


# Helper methods to create distributions
class Distributions:
    NONE = DistributionImpl(Strategy.NONE, 0, [])
    HASH = DistributionImpl(Strategy.HASH, 0, [])
    RANGE = DistributionImpl(Strategy.RANGE, 0, [])

    @staticmethod
    def even(number: int, *expressions) -> Distribution:
        return DistributionImpl(Strategy.EVEN, number, list(expressions))

    @staticmethod
    def hash(number: int, *expressions) -> Distribution:
        return DistributionImpl(Strategy.HASH, number, list(expressions))

    @staticmethod
    def of(strategy: str, number: int, *expressions) -> Distribution:
        return DistributionImpl(strategy, number, list(expressions))

    @staticmethod
    def fields(strategy: str, number: int, *field_names: Tuple[str]) -> Distribution:
        expressions = [NamedReference.field(field_name) for field_name in field_names]
        return Distributions.of(strategy, number, *expressions)
