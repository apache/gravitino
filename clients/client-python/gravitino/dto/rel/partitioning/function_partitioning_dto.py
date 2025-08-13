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

from gravitino.api.expressions.expression import Expression
from gravitino.dto.rel.column_dto import ColumnDTO
from gravitino.dto.rel.expressions.function_arg import FunctionArg
from gravitino.dto.rel.partition_utils import PartitionUtils
from gravitino.dto.rel.partitioning.partitioning import Partitioning


class FunctionPartitioningDTO(Partitioning):
    """Data transfer object for function partitioning."""

    def __init__(self, function_name: str, /, *function_arg: FunctionArg):
        self._function_name = function_name
        self._args = [*function_arg]

    def function_name(self) -> str:
        """Returns the name of the function.

        Returns:
            str: The name of the function.
        """
        return self._function_name

    def args(self) -> List[FunctionArg]:
        """Returns the arguments of the function.

        Returns:
            List[FunctionArg]: The arguments of the function.
        """
        return self._args

    def strategy(self) -> Partitioning.Strategy:
        return self.Strategy.FUNCTION

    def validate(self, columns: List[ColumnDTO]) -> None:
        for ref in self.references():
            PartitionUtils.validate_field_existence(columns, ref.field_name())

    def name(self) -> str:
        return self._function_name

    def arguments(self) -> List[Expression]:
        """Returns the arguments of the function.

        Returns:
            List[Expression]: The arguments of the function.
        """
        return self._args
