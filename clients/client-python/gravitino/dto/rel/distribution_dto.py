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

from gravitino.api.expressions.distributions.distribution import Distribution
from gravitino.api.expressions.distributions.strategy import Strategy
from gravitino.dto.rel.column_dto import ColumnDTO
from gravitino.dto.rel.expressions.function_arg import FunctionArg
from gravitino.utils.precondition import Precondition


class DistributionDTO(Distribution):
    """Data transfer object representing distribution information.

    Attributes:
        NONE (DistributionDTO):
            A DistributionDTO instance that represents no distribution.
    """

    NONE: "DistributionDTO"

    def __init__(
        self,
        strategy: Strategy,
        number: int,
        args: List[FunctionArg],
    ):
        Precondition.check_argument(
            number >= -1, "bucketNum must be greater than or equal -1"
        )
        Precondition.check_argument(args is not None, "expressions cannot be null")
        self._strategy = strategy if isinstance(strategy, Strategy) else Strategy.HASH
        self._number = number
        self._args = args

    def args(self) -> List[FunctionArg]:
        """Returns the arguments of the function.

        Returns:
            List[FunctionArg]: The arguments of the function.
        """
        return self._args

    def strategy(self) -> Strategy:
        """Returns the strategy of the distribution.

        Returns:
            Strategy: The strategy of the distribution.
        """
        return self._strategy

    def number(self) -> int:
        """Returns the number of buckets.

        Returns:
            int: The number of buckets.
        """
        return self._number

    def expressions(self) -> List[FunctionArg]:
        """Returns the name of the distribution.

        Returns:
            List[FunctionArg]: The name of the distribution.
        """
        return self._args

    def validate(self, columns: List[ColumnDTO]) -> None:
        """Validates the distribution.

        Args:
            columns (List[ColumnDTO]): The columns to be validated.

        Raises:
            IllegalArgumentException: If the distribution is invalid.
        """

        for expression in self._args:
            expression.validate(columns)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, DistributionDTO):
            return False
        return self is other or (
            self._number == other.number()
            and self._args == other.args()
            and self._strategy is other.strategy()
        )

    def __hash__(self) -> int:
        result = hash(tuple(self._args))
        result = 31 * result + self._number
        result = 31 * result + hash(self._strategy) if self._strategy else 0
        return result


DistributionDTO.NONE = DistributionDTO(Strategy.NONE, 0, FunctionArg.EMPTY_ARGS)
