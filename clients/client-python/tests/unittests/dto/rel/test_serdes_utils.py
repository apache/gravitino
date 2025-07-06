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

import unittest
from enum import Enum
from unittest.mock import patch

from gravitino.api.types.types import Types
from gravitino.dto.rel.expressions.field_reference_dto import FieldReferenceDTO
from gravitino.dto.rel.expressions.json_serdes._helper.serdes_utils import SerdesUtils
from gravitino.dto.rel.expressions.literal_dto import LiteralDTO


class MockArgType(str, Enum):
    INVALID_ARG_TYPE = "invalid_arg_type"


class TestExpressionSerdesUtils(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._literal_dto = (
            LiteralDTO.builder()
            .with_data_type(data_type=Types.StringType.get())
            .with_value(value="test_string")
            .build()
        )
        cls._field_reference_dto = (
            FieldReferenceDTO.builder()
            .with_field_name(field_name=["field_name"])
            .build()
        )

    def test_write_function_arg_invalid_arg_type(self):
        mock_dto = (
            LiteralDTO.builder()
            .with_data_type(data_type=Types.StringType.get())
            .with_value(value="test")
            .build()
        )
        with patch.object(
            mock_dto, "arg_type", return_value=MockArgType.INVALID_ARG_TYPE
        ):
            self.assertRaises(ValueError, SerdesUtils.write_function_arg, arg=mock_dto)

    def test_write_function_arg_literal_dto(self):
        result = SerdesUtils.write_function_arg(arg=self._literal_dto)
        expected_result = {
            SerdesUtils.EXPRESSION_TYPE: self._literal_dto.arg_type().name.lower(),
            SerdesUtils.DATA_TYPE: self._literal_dto.data_type().simple_string(),
            SerdesUtils.LITERAL_VALUE: self._literal_dto.value(),
        }
        self.assertDictEqual(result, expected_result)

    def test_write_function_arg_field_reference_dto(self):
        result = SerdesUtils.write_function_arg(arg=self._field_reference_dto)
        expected_result = {
            SerdesUtils.EXPRESSION_TYPE: self._field_reference_dto.arg_type().name.lower(),
            SerdesUtils.FIELD_NAME: self._field_reference_dto.field_name(),
        }
        self.assertDictEqual(result, expected_result)
