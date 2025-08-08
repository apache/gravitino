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

from typing import Any, Dict, cast

from gravitino.api.types.json_serdes._helper.serdes_utils import (
    SerdesUtils as TypesSerdesUtils,
)
from gravitino.dto.rel.expressions.field_reference_dto import FieldReferenceDTO
from gravitino.dto.rel.expressions.func_expression_dto import FuncExpressionDTO
from gravitino.dto.rel.expressions.function_arg import FunctionArg
from gravitino.dto.rel.expressions.literal_dto import LiteralDTO
from gravitino.dto.rel.expressions.unparsed_expression_dto import UnparsedExpressionDTO
from gravitino.exceptions.base import IllegalArgumentException
from gravitino.utils.precondition import Precondition
from gravitino.utils.serdes import SerdesUtilsBase


class SerdesUtils(SerdesUtilsBase):
    @classmethod
    def write_function_arg(cls, arg: FunctionArg) -> Dict[str, Any]:
        arg_type = arg.arg_type()
        if arg_type not in FunctionArg.ArgType:
            raise ValueError(f"Unknown function argument type: {arg_type}")

        arg_data = {cls.EXPRESSION_TYPE: arg_type.name.lower()}
        if arg_type is FunctionArg.ArgType.LITERAL:
            expression = cast(LiteralDTO, arg)
            arg_data[cls.DATA_TYPE] = TypesSerdesUtils.write_data_type(
                data_type=expression.data_type()
            )
            arg_data[cls.LITERAL_VALUE] = expression.value()

        if arg_type is FunctionArg.ArgType.FIELD:
            arg_data[cls.FIELD_NAME] = cast(FieldReferenceDTO, arg).field_name()

        if arg_type is FunctionArg.ArgType.FUNCTION:
            expression = cast(FuncExpressionDTO, arg)
            arg_data[cls.FUNCTION_NAME] = expression.function_name()
            arg_data[cls.FUNCTION_ARGS] = [
                cls.write_function_arg(func_arg) for func_arg in expression.args()
            ]

        if arg_type is FunctionArg.ArgType.UNPARSED:
            expression = cast(UnparsedExpressionDTO, arg)
            arg_data[cls.UNPARSED_EXPRESSION] = expression.unparsed_expression()

        return arg_data

    @classmethod
    def read_function_arg(cls, data: Dict[str, Any]) -> FunctionArg:
        Precondition.check_argument(
            data is not None and isinstance(data, dict),
            f"Cannot parse function arg from invalid JSON: {data}",
        )
        Precondition.check_argument(
            data.get(cls.EXPRESSION_TYPE) is not None,
            f"Cannot parse function arg from missing type: {data}",
        )
        try:
            arg_type = FunctionArg.ArgType(data[cls.EXPRESSION_TYPE].lower())
        except ValueError:
            raise IllegalArgumentException(
                f"Unknown function argument type: {data[cls.EXPRESSION_TYPE]}"
            )

        if arg_type is FunctionArg.ArgType.LITERAL:
            Precondition.check_argument(
                data.get(cls.DATA_TYPE) is not None,
                f"Cannot parse literal arg from missing data type: {data}",
            )
            Precondition.check_argument(
                data.get(cls.LITERAL_VALUE) is not None,
                f"Cannot parse literal arg from missing literal value: {data}",
            )
            return (
                LiteralDTO.builder()
                .with_data_type(
                    data_type=TypesSerdesUtils.read_data_type(data[cls.DATA_TYPE])
                )
                .with_value(value=data[cls.LITERAL_VALUE])
                .build()
            )

        if arg_type is FunctionArg.ArgType.FIELD:
            Precondition.check_argument(
                data.get(cls.FIELD_NAME) is not None,
                f"Cannot parse field reference arg from missing field name: {data}",
            )
            return (
                FieldReferenceDTO.builder()
                .with_field_name(field_name=data[cls.FIELD_NAME])
                .build()
            )

        if arg_type is FunctionArg.ArgType.FUNCTION:
            Precondition.check_argument(
                data.get(cls.FUNCTION_NAME) is not None,
                f"Cannot parse function function arg from missing function name: {data}",
            )
            Precondition.check_argument(
                data.get(cls.FUNCTION_ARGS) is not None,
                f"Cannot parse function function arg from missing function args: {data}",
            )
            args = [cls.read_function_arg(arg) for arg in data[cls.FUNCTION_ARGS]]
            return (
                FuncExpressionDTO.builder()
                .with_function_name(function_name=data[cls.FUNCTION_NAME])
                .with_function_args(function_args=args or FunctionArg.EMPTY_ARGS)
                .build()
            )

        if arg_type is FunctionArg.ArgType.UNPARSED:
            Precondition.check_argument(
                isinstance(data.get(cls.UNPARSED_EXPRESSION), str),
                f"Cannot parse unparsed expression from missing string field unparsedExpression: {data}",
            )
            return UnparsedExpressionDTO(data[cls.UNPARSED_EXPRESSION])
