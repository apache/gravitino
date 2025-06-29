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

from __future__ import annotations

from typing import List

from gravitino.api.expressions.expression import Expression
from gravitino.api.expressions.function_expression import FunctionExpression
from gravitino.dto.rel.expressions.function_arg import FunctionArg


class FuncExpressionDTO(FunctionExpression, FunctionArg):
    def __init__(self, function_name: str, function_args: List[FunctionArg]):
        self._function_name = function_name
        self._function_args = function_args

    def args(self) -> List[FunctionArg]:
        """The function arguments.

        Returns:
            List[FunctionArg]: The function arguments.
        """
        return self._function_args

    def function_name(self) -> str:
        return self._function_name

    def arguments(self) -> List[Expression]:
        return self._function_args

    def arg_type(self) -> FunctionArg.ArgType:
        return self.ArgType.FUNCTION

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, FuncExpressionDTO):
            return False
        return (
            self._function_name == other.function_name()
            and self._function_args == other.args()
            and self.arg_type() is other.arg_type()
        )

    def __hash__(self) -> int:
        return hash((self.arg_type(), self._function_name, tuple(self._function_args)))
