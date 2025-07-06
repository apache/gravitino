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

from typing import Any, ClassVar, Dict, cast

from gravitino.api.types.json_serdes._helper.serdes_utils import (
    SerdesUtils as TypesSerdesUtils,
)
from gravitino.dto.rel.expressions.field_reference_dto import FieldReferenceDTO
from gravitino.dto.rel.expressions.function_arg import FunctionArg
from gravitino.dto.rel.expressions.literal_dto import LiteralDTO


class SerdesUtils:
    EXPRESSION_TYPE: ClassVar[str] = "type"
    DATA_TYPE: ClassVar[str] = "dataType"
    LITERAL_VALUE: ClassVar[str] = "value"
    FIELD_NAME: ClassVar[str] = "fieldName"

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

        return arg_data
