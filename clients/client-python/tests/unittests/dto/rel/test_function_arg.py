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

from gravitino.api.types.types import Types
from gravitino.dto.rel.column_dto import ColumnDTO
from gravitino.dto.rel.expressions.function_arg import FunctionArg
from gravitino.dto.rel.expressions.literal_dto import LiteralDTO


class TestFunctionArg(unittest.TestCase):
    def setUp(self) -> None:
        self._data_types = [
            Types.StringType.get(),
            Types.IntegerType.get(),
            Types.DateType.get(),
        ]
        self._column_names = [f"column{i}" for i in range(len(self._data_types))]
        self._columns = [
            ColumnDTO.builder(
                name=column_name,
                data_type=data_type,
                comment=f"{column_name} comment",
                nullable=False,
            )
            for column_name, data_type in zip(self._column_names, self._data_types)
        ]

    def test_function_arg(self):
        self.assertEqual(FunctionArg.EMPTY_ARGS, [])

    def test_function_arg_validate(self):
        LiteralDTO(data_type=Types.StringType.get(), value="test").validate(
            columns=self._columns
        )
        # TODO: add unit test for FunctionArg with children
