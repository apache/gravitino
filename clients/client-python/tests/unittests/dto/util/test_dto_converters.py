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
from datetime import datetime
from random import randint, random
from typing import cast
from unittest.mock import MagicMock, patch

from gravitino.api.rel.expressions.distributions.distributions import Distributions
from gravitino.api.rel.expressions.distributions.strategy import Strategy
from gravitino.api.rel.expressions.expression import Expression
from gravitino.api.rel.expressions.function_expression import FunctionExpression
from gravitino.api.rel.expressions.literals.literals import Literals
from gravitino.api.rel.expressions.named_reference import FieldReference
from gravitino.api.rel.expressions.unparsed_expression import UnparsedExpression
from gravitino.api.rel.indexes.index import Index
from gravitino.api.rel.indexes.indexes import Indexes
from gravitino.api.rel.types.types import Types
from gravitino.dto.rel.distribution_dto import DistributionDTO
from gravitino.dto.rel.expressions.field_reference_dto import FieldReferenceDTO
from gravitino.dto.rel.expressions.func_expression_dto import FuncExpressionDTO
from gravitino.dto.rel.expressions.function_arg import FunctionArg
from gravitino.dto.rel.expressions.literal_dto import LiteralDTO
from gravitino.dto.rel.expressions.unparsed_expression_dto import UnparsedExpressionDTO
from gravitino.dto.rel.indexes.index_dto import IndexDTO
from gravitino.dto.util.dto_converters import DTOConverters
from gravitino.exceptions.base import IllegalArgumentException


class TestDTOConverters(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        random_integer = randint(-100, 100)
        random_float = round(random() * 100, 2)
        cls.literals = {
            Types.NullType.get(): None,
            Types.BooleanType.get(): True,
            Types.IntegerType.get(): random_integer,
            Types.DoubleType.get(): random_float,
            Types.StringType.get(): "test",
            Types.FloatType.get(): random_float,
            Types.ShortType.get(): random_integer,
            Types.LongType.get(): random_integer,
            Types.DecimalType.of(10, 2): random_float,
            Types.DateType.get(): "2025-10-14",
            Types.TimestampType.without_time_zone(): datetime.fromisoformat(
                "2025-10-14T00:00:00.000"
            ),
            Types.TimeType.get(): "11:34:58.123",
            Types.BinaryType.get(): bytes("test", "utf-8"),
            Types.VarCharType.of(10): "test",
            Types.FixedCharType.of(10): "test",
        }

    def test_from_function_arg_literal_dto(self):
        for data_type, value in TestDTOConverters.literals.items():
            expected = Literals.of(value=value, data_type=data_type)
            literal_dto = (
                LiteralDTO.builder().with_data_type(data_type).with_value(value).build()
            )
            self.assertTrue(DTOConverters.from_function_arg(literal_dto) == expected)

    def test_from_function_arg_func_expression_dto(self):
        function_name = "test_function"
        args: list[FunctionArg] = [
            LiteralDTO.builder()
            .with_data_type(Types.IntegerType.get())
            .with_value("-1")
            .build(),
            LiteralDTO.builder()
            .with_data_type(Types.BooleanType.get())
            .with_value("True")
            .build(),
        ]
        func_expression_dto = (
            FuncExpressionDTO.builder()
            .with_function_name(function_name)
            .with_function_args(args)
            .build()
        )
        expected = FunctionExpression.of(
            function_name,
            Literals.of(value="-1", data_type=Types.IntegerType.get()),
            Literals.of(value="True", data_type=Types.BooleanType.get()),
        )
        converted = DTOConverters.from_function_arg(func_expression_dto)
        self.assertIsInstance(converted, FunctionExpression)
        converted = cast(FunctionExpression, converted)
        self.assertEqual(converted.function_name(), expected.function_name())
        self.assertListEqual(converted.arguments(), expected.arguments())

    def test_from_function_arg_field_reference(self):
        field_names = [f"field_{i}" for i in range(3)]
        field_reference_dto = (
            FieldReferenceDTO.builder().with_field_name(field_name=field_names).build()
        )
        expected = FieldReference(field_names=field_names)
        converted = DTOConverters.from_function_arg(field_reference_dto)
        self.assertIsInstance(converted, FieldReference)
        self.assertTrue(converted == expected)

    def test_from_function_arg_unparsed(self):
        expected = UnparsedExpression.of(unparsed_expression="unparsed")
        unparsed_expression_dto = (
            UnparsedExpressionDTO.builder().with_unparsed_expression("unparsed").build()
        )
        converted = DTOConverters.from_function_arg(unparsed_expression_dto)
        self.assertIsInstance(converted, UnparsedExpression)
        self.assertTrue(converted == expected)

    @patch.object(FunctionArg, "arg_type", return_value="invalid_type")
    def test_from_function_arg_raises_exception(
        self, mock_function_arg_arg_type: MagicMock
    ):
        with self.assertRaisesRegex(
            IllegalArgumentException, "Unsupported expression type"
        ):
            DTOConverters.from_function_arg(mock_function_arg_arg_type)

    def test_from_function_args(self):
        args = [
            LiteralDTO.builder()
            .with_data_type(Types.IntegerType.get())
            .with_value("-1")
            .build(),
            LiteralDTO.builder()
            .with_data_type(Types.BooleanType.get())
            .with_value("True")
            .build(),
        ]
        expected = [
            Literals.of(value="-1", data_type=Types.IntegerType.get()),
            Literals.of(value="True", data_type=Types.BooleanType.get()),
        ]
        converted = DTOConverters.from_function_args(args)
        self.assertListEqual(converted, expected)
        self.assertListEqual(
            DTOConverters.from_function_args([]), Expression.EMPTY_EXPRESSION
        )

    def test_from_dto_distribution(self):
        field_names = [f"field_{i}" for i in range(2)]
        field_ref_dtos = [
            FieldReferenceDTO.builder().with_field_name(field_name=[field_name]).build()
            for field_name in field_names
        ]
        distribution_dto = DistributionDTO(
            strategy=Strategy.HASH, number=4, args=field_ref_dtos
        )
        expected = Distributions.of(
            Strategy.HASH, 4, *DTOConverters.from_function_args(field_ref_dtos)
        )
        converted = DTOConverters.from_dto(distribution_dto)
        self.assertTrue(converted == expected)
        self.assertTrue(
            DTOConverters.from_dto(DistributionDTO.NONE) == Distributions.NONE
        )

    def test_from_dto_index(self):
        field_names = [[f"field_{i}"] for i in range(2)]

        index_dto = IndexDTO(
            index_type=Index.IndexType.PRIMARY_KEY,
            name="PRIMARY",
            field_names=field_names,
        )
        converted = DTOConverters.from_dto(index_dto)
        expected = Indexes.of(Index.IndexType.PRIMARY_KEY, "PRIMARY", field_names)
        self.assertTrue(converted.type() == expected.type())
        self.assertTrue(converted.name() == expected.name())
        self.assertListEqual(converted.field_names(), expected.field_names())

    def test_from_dto_raises_exception(self):
        with self.assertRaisesRegex(
            IllegalArgumentException, "Unsupported expression type"
        ):
            DTOConverters.from_dto("")
