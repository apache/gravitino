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
from datetime import date, datetime
from itertools import product
from random import randint, random
from typing import Dict, cast
from unittest.mock import MagicMock, patch

from gravitino.api.rel.column import Column
from gravitino.api.rel.expressions.distributions.distributions import Distributions
from gravitino.api.rel.expressions.distributions.strategy import Strategy
from gravitino.api.rel.expressions.expression import Expression
from gravitino.api.rel.expressions.function_expression import FunctionExpression
from gravitino.api.rel.expressions.literals.literals import Literals
from gravitino.api.rel.expressions.named_reference import FieldReference
from gravitino.api.rel.expressions.sorts.null_ordering import NullOrdering
from gravitino.api.rel.expressions.sorts.sort_direction import SortDirection
from gravitino.api.rel.expressions.sorts.sort_orders import SortOrders
from gravitino.api.rel.expressions.transforms.transforms import Transforms
from gravitino.api.rel.expressions.unparsed_expression import UnparsedExpression
from gravitino.api.rel.indexes.index import Index
from gravitino.api.rel.indexes.indexes import Indexes
from gravitino.api.rel.partitions.partition import Partition
from gravitino.api.rel.partitions.partitions import Partitions
from gravitino.api.rel.table import Table
from gravitino.api.rel.types.types import Types
from gravitino.dto.rel.column_dto import ColumnDTO
from gravitino.dto.rel.distribution_dto import DistributionDTO
from gravitino.dto.rel.expressions.field_reference_dto import FieldReferenceDTO
from gravitino.dto.rel.expressions.func_expression_dto import FuncExpressionDTO
from gravitino.dto.rel.expressions.function_arg import FunctionArg
from gravitino.dto.rel.expressions.literal_dto import LiteralDTO
from gravitino.dto.rel.expressions.unparsed_expression_dto import UnparsedExpressionDTO
from gravitino.dto.rel.indexes.index_dto import IndexDTO
from gravitino.dto.rel.partitioning.bucket_partitioning_dto import BucketPartitioningDTO
from gravitino.dto.rel.partitioning.day_partitioning_dto import DayPartitioningDTO
from gravitino.dto.rel.partitioning.function_partitioning_dto import (
    FunctionPartitioningDTO,
)
from gravitino.dto.rel.partitioning.hour_partitioning_dto import HourPartitioningDTO
from gravitino.dto.rel.partitioning.identity_partitioning_dto import (
    IdentityPartitioningDTO,
)
from gravitino.dto.rel.partitioning.list_partitioning_dto import ListPartitioningDTO
from gravitino.dto.rel.partitioning.month_partitioning_dto import MonthPartitioningDTO
from gravitino.dto.rel.partitioning.partitioning import Partitioning
from gravitino.dto.rel.partitioning.range_partitioning_dto import RangePartitioningDTO
from gravitino.dto.rel.partitioning.truncate_partitioning_dto import (
    TruncatePartitioningDTO,
)
from gravitino.dto.rel.partitioning.year_partitioning_dto import YearPartitioningDTO
from gravitino.dto.rel.partitions.identity_partition_dto import IdentityPartitionDTO
from gravitino.dto.rel.partitions.list_partition_dto import ListPartitionDTO
from gravitino.dto.rel.partitions.range_partition_dto import RangePartitionDTO
from gravitino.dto.rel.sort_order_dto import SortOrderDTO
from gravitino.dto.rel.table_dto import TableDTO
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
        cls.function_args = {
            FieldReference(field_names=["score"]): FieldReferenceDTO.builder()
            .with_field_name(field_name=["score"])
            .build(),
            UnparsedExpression.of(
                unparsed_expression="unparsed"
            ): UnparsedExpressionDTO.builder()
            .with_unparsed_expression("unparsed")
            .build(),
        }
        cls.table_dto_json = """
        {
            "name": "example_table",
            "comment": "This is an example table",
            "audit": {
                "creator": "Apache Gravitino",
                "createTime":"2025-10-10T00:00:00"
            },
            "columns": [
                {
                    "name": "id",
                    "type": "integer",
                    "comment": "id column comment",
                    "nullable": false,
                    "autoIncrement": true,
                    "defaultValue": {
                        "type": "literal",
                        "dataType": "integer",
                        "value": "-1"
                    }
                },
                {
                    "name": "name",
                    "type": "varchar(500)",
                    "comment": "name column comment",
                    "nullable": true,
                    "autoIncrement": false,
                    "defaultValue": {
                        "type": "literal",
                        "dataType": "null",
                        "value": "null"
                    }
                },
                {
                    "name": "StartingDate",
                    "type": "timestamp",
                    "comment": "StartingDate column comment",
                    "nullable": false,
                    "autoIncrement": false,
                    "defaultValue": {
                        "type": "function",
                        "funcName": "current_timestamp",
                        "funcArgs": []
                    }
                },
                {
                    "name": "info",
                    "type": {
                        "type": "struct",
                        "fields": [
                            {
                                "name": "position",
                                "type": "string",
                                "nullable": true,
                                "comment": "position field comment"
                            },
                            {
                                "name": "contact",
                                "type": {
                                "type": "list",
                                "elementType": "integer",
                                "containsNull": false
                                },
                                "nullable": true,
                                "comment": "contact field comment"
                            },
                            {
                                "name": "rating",
                                "type": {
                                "type": "map",
                                "keyType": "string",
                                "valueType": "integer",
                                "valueContainsNull": false
                                },
                                "nullable": true,
                                "comment": "rating field comment"
                            }
                        ]
                    },
                    "comment": "info column comment",
                    "nullable": true
                },
                {
                    "name": "dt",
                    "type": "date",
                    "comment": "dt column comment",
                    "nullable": true
                }
            ],
            "partitioning": [
                {
                    "strategy": "identity",
                    "fieldName": [ "dt" ]
                }
            ],
            "distribution": {
                "strategy": "hash",
                "number": 32,
                "funcArgs": [
                    {
                        "type": "field",
                        "fieldName": [ "id" ]
                    }
                ]
            },
            "sortOrders": [
                {
                    "sortTerm": {
                        "type": "field",
                        "fieldName": [ "age" ]
                    },
                    "direction": "asc",
                    "nullOrdering": "nulls_first"
                }
            ],
            "indexes": [
                {
                    "indexType": "primary_key",
                    "name": "PRIMARY",
                    "fieldNames": [["id"]]
                }
            ],
            "properties": {
                "format": "ORC"
            }
        }
        """

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
        with self.assertRaisesRegex(IllegalArgumentException, "Unsupported DTO type"):
            DTOConverters.from_dto("")

    def test_from_dto_sort_order(self):
        direction, null_ordering = SortDirection.ASCENDING, NullOrdering.NULLS_LAST
        field_ref_dto = (
            FieldReferenceDTO.builder().with_field_name(field_name=["score"]).build()
        )
        sort_order_dto = SortOrderDTO(
            sort_term=field_ref_dto,
            direction=direction,
            null_ordering=null_ordering,
        )
        expected = SortOrders.of(
            expression=DTOConverters.from_function_arg(field_ref_dto),
            direction=direction,
            null_ordering=null_ordering,
        )
        converted = DTOConverters.from_dto(sort_order_dto)
        self.assertTrue(converted == expected)

    def test_from_dto_column(self):
        column_name = "test_column"
        column_data_type = Types.IntegerType.get()

        # Test for default value not set
        column_dto = (
            ColumnDTO.builder()
            .with_name(column_name)
            .with_data_type(column_data_type)
            .build()
        )
        converted = DTOConverters.from_dto(column_dto)
        self.assertTrue(converted.default_value() == Column.DEFAULT_VALUE_NOT_SET)
        self.assertTrue(converted == column_dto)

        column_dto = (
            ColumnDTO.builder()
            .with_name(column_name)
            .with_data_type(column_data_type)
            .with_default_value(
                LiteralDTO.builder()
                .with_data_type(column_data_type)
                .with_value("1")
                .build()
            )
            .build()
        )
        expected = Column.of(
            name=column_name,
            data_type=column_data_type,
            default_value=Literals.of(value="1", data_type=column_data_type),
        )
        converted = DTOConverters.from_dto(column_dto)
        self.assertTrue(converted.default_value() != Column.DEFAULT_VALUE_NOT_SET)
        self.assertTrue(expected.default_value() != Column.DEFAULT_VALUE_NOT_SET)
        self.assertTrue(converted == expected)

    def test_from_dto_partitioning(self):
        field_name = ["score"]
        field_names = [field_name]
        partitioning = {
            Partitioning.Strategy.IDENTITY: IdentityPartitioningDTO(*field_name),
            Partitioning.Strategy.YEAR: YearPartitioningDTO(*field_name),
            Partitioning.Strategy.MONTH: MonthPartitioningDTO(*field_name),
            Partitioning.Strategy.DAY: DayPartitioningDTO(*field_name),
            Partitioning.Strategy.HOUR: HourPartitioningDTO(*field_name),
            Partitioning.Strategy.BUCKET: BucketPartitioningDTO(10, *field_names),
            Partitioning.Strategy.TRUNCATE: TruncatePartitioningDTO(10, field_name),
            Partitioning.Strategy.LIST: ListPartitioningDTO([["createTime"], ["city"]]),
            Partitioning.Strategy.RANGE: RangePartitioningDTO(field_name),
            Partitioning.Strategy.FUNCTION: FunctionPartitioningDTO(
                "test_function",
                LiteralDTO.builder()
                .with_data_type(Types.IntegerType.get())
                .with_value("-1")
                .build(),
                LiteralDTO.builder()
                .with_data_type(Types.BooleanType.get())
                .with_value("True")
                .build(),
            ),
        }
        transform = {
            Partitioning.Strategy.IDENTITY: Transforms.identity(field_name),
            Partitioning.Strategy.YEAR: Transforms.year(field_name),
            Partitioning.Strategy.MONTH: Transforms.month(field_name),
            Partitioning.Strategy.DAY: Transforms.day(field_name),
            Partitioning.Strategy.HOUR: Transforms.hour(field_name),
            Partitioning.Strategy.BUCKET: Transforms.bucket(10, *field_names),
            Partitioning.Strategy.TRUNCATE: Transforms.truncate(10, field_name),
            Partitioning.Strategy.LIST: Transforms.list(["createTime"], ["city"]),
            Partitioning.Strategy.RANGE: Transforms.range(field_name),
            Partitioning.Strategy.FUNCTION: Transforms.apply(
                "test_function",
                [
                    Literals.of(value="-1", data_type=Types.IntegerType.get()),
                    Literals.of(value="True", data_type=Types.BooleanType.get()),
                ],
            ),
        }

        for strategy, dto in partitioning.items():
            expected = transform[strategy]
            self.assertTrue(DTOConverters.from_dto(dto) == expected)

        with (
            patch.object(IdentityPartitioningDTO, "strategy") as mock_strategy,
            self.assertRaisesRegex(
                IllegalArgumentException, "Unsupported partitioning"
            ),
        ):
            mock_strategy.return_value = "invalid_strategy"
            DTOConverters.from_dto(IdentityPartitioningDTO(*field_name))

    def test_from_dtos_index_dto(self):
        field_names = [[f"field_{i}"] for i in range(2)]

        dtos = [
            IndexDTO(
                index_type=index_type,
                name=index_type.value,
                field_names=field_names,
            )
            for index_type in Index.IndexType
        ]
        converted_dtos = DTOConverters.from_dtos(dtos)
        expected_items = [
            Indexes.of(index_type, index_type.value, field_names)
            for index_type in Index.IndexType
        ]
        self.assertEqual(len(converted_dtos), len(expected_items))
        for converted, expected in zip(converted_dtos, expected_items):
            self.assertTrue(converted.type() == expected.type())
            self.assertTrue(converted.name() == expected.name())
            self.assertListEqual(converted.field_names(), expected.field_names())

        self.assertEqual(DTOConverters.from_dtos([]), [])

    def test_from_dtos_sort_order(self):
        directions = {SortDirection.ASCENDING, SortDirection.DESCENDING}
        null_orderings = {NullOrdering.NULLS_LAST, NullOrdering.NULLS_FIRST}
        field_names = [
            [f"score_{i}"] for i in range(len(directions) * len(null_orderings))
        ]
        sort_order_dtos = []
        expected = []
        for field_name, (direction, null_ordering) in zip(
            field_names, product(directions, null_orderings)
        ):
            field_ref_dto = (
                FieldReferenceDTO.builder()
                .with_field_name(field_name=field_name)
                .build()
            )
            sort_order_dtos.append(
                SortOrderDTO(
                    sort_term=field_ref_dto,
                    direction=direction,
                    null_ordering=null_ordering,
                )
            )
            expected.append(
                SortOrders.of(
                    expression=DTOConverters.from_function_arg(field_ref_dto),
                    direction=direction,
                    null_ordering=null_ordering,
                )
            )
        converted = DTOConverters.from_dtos(sort_order_dtos)
        self.assertListEqual(converted, expected)

    def test_from_dtos_partitioning(self):
        field_name = ["score"]
        field_names = [field_name]
        partitioning = [
            IdentityPartitioningDTO(*field_name),
            BucketPartitioningDTO(10, *field_names),
        ]
        transform = [
            Transforms.identity(field_name),
            Transforms.bucket(10, *field_names),
        ]
        converted = DTOConverters.from_dtos(partitioning)
        self.assertListEqual(converted, transform)

    def test_from_dtos_column_dto(self):
        column_names = {f"column_{i}" for i in range(2)}
        column_data_types = {Types.IntegerType.get(), Types.BooleanType.get()}
        column_dtos: list[ColumnDTO] = [
            ColumnDTO.builder()
            .with_name(column_name)
            .with_data_type(column_data_type)
            .build()
            for column_name, column_data_type in zip(column_names, column_data_types)
        ]
        self.assertListEqual(DTOConverters.from_dtos(column_dtos), column_dtos)

    def test_from_dtos_column_dto_with_default_value(self):
        column_names = {f"column_{i}" for i in range(2)}
        column_data_types = {Types.IntegerType.get(), Types.BooleanType.get()}
        column_dto_default_values = {
            LiteralDTO.builder()
            .with_data_type(Types.IntegerType.get())
            .with_value("1")
            .build(),
            LiteralDTO.builder()
            .with_data_type(Types.BooleanType.get())
            .with_value("True")
            .build(),
        }
        column_dtos: list[ColumnDTO] = [
            ColumnDTO.builder()
            .with_name(column_name)
            .with_data_type(column_data_type)
            .with_default_value(default_value)
            .build()
            for column_name, column_data_type, default_value in zip(
                column_names, column_data_types, column_dto_default_values
            )
        ]
        expected = [DTOConverters.from_dto(dto) for dto in column_dtos]
        converted = DTOConverters.from_dtos(column_dtos)
        self.assertListEqual(converted, expected)

    def test_from_dto_table_dto(self):
        dto = TableDTO.from_json(self.table_dto_json)
        converted = DTOConverters.from_dto(dto)
        table = cast(Table, converted)
        self.assertIsInstance(converted, Table)
        self.assertEqual(table.name(), dto.name())
        self.assertEqual(table.comment(), dto.comment())
        self.assertListEqual(table.columns(), DTOConverters.from_dtos(dto.columns()))
        self.assertListEqual(
            table.partitioning(), DTOConverters.from_dtos(dto.partitioning())
        )
        self.assertListEqual(
            table.sort_order(), DTOConverters.from_dtos(dto.sort_order())
        )
        for table_index, dto_index in zip(
            table.index(), DTOConverters.from_dtos(dto.index())
        ):
            self.assertEqual(table_index.name(), dto_index.name())
            self.assertEqual(table_index.type(), dto_index.type())
            self.assertListEqual(table_index.field_names(), dto_index.field_names())
        self.assertEqual(
            table.distribution(), DTOConverters.from_dto(dto.distribution())
        )
        self.assertEqual(table.audit_info(), dto.audit_info())
        self.assertEqual(table.properties(), dto.properties())

    def test_to_function_arg_function_arg(self):
        for expression, function_arg in TestDTOConverters.function_args.items():
            converted = DTOConverters.to_function_arg(function_arg)
            self.assertTrue(converted == function_arg)
            converted = DTOConverters.to_function_arg(expression)
            self.assertTrue(converted == function_arg)

        for data_type, value in TestDTOConverters.literals.items():
            literal = Literals.of(value=value, data_type=data_type)
            expected = (
                LiteralDTO.builder()
                .with_data_type(data_type)
                .with_value(str(value))
                .build()
            )
            self.assertTrue(DTOConverters.to_function_arg(literal) == expected)
        self.assertTrue(DTOConverters.to_function_arg(Literals.NULL) == LiteralDTO.NULL)

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
        expected = (
            FuncExpressionDTO.builder()
            .with_function_name(function_name)
            .with_function_args(args)
            .build()
        )
        func_expression = FunctionExpression.of(
            function_name,
            Literals.of(value="-1", data_type=Types.IntegerType.get()),
            Literals.of(value="True", data_type=Types.BooleanType.get()),
        )
        converted = DTOConverters.to_function_arg(func_expression)
        self.assertTrue(converted == expected)

        with self.assertRaisesRegex(
            IllegalArgumentException, "Unsupported expression type"
        ):
            DTOConverters.to_function_arg(DistributionDTO.NONE)

    def test_to_dto_raise_exception(self):
        with self.assertRaisesRegex(IllegalArgumentException, "Unsupported type"):
            DTOConverters.to_dto("test")

    def test_to_dto_range_partition(self):
        converted = DTOConverters.to_dto(
            Partitions.range(
                name="p0",
                upper=Literals.NULL,
                lower=Literals.integer_literal(6),
                properties={},
            )
        )
        expected = RangePartitionDTO(
            name="p0",
            upper=LiteralDTO.NULL,
            lower=LiteralDTO.builder()
            .with_value("6")
            .with_data_type(Types.IntegerType.get())
            .build(),
            properties={},
        )
        self.assertTrue(converted == expected)

    def test_to_dto_partition_raise_exception(self):
        class InvalidPartition(Partition):
            def name(self) -> str:
                return "invalid_partition"

            def properties(self) -> Dict[str, str]:
                return {}

        with self.assertRaisesRegex(
            IllegalArgumentException, "Unsupported partition type"
        ):
            DTOConverters.to_dto(InvalidPartition())

    def test_to_dto_identity_partition(self):
        partition_name = "dt=2025-08-08/country=us"
        field_names = [["dt"], ["country"]]
        properties = {"location": "/user/hive/warehouse/tpch_flat_orc_2.db/orders"}
        values = [
            LiteralDTO.builder()
            .with_data_type(data_type=Types.DateType.get())
            .with_value(value="2025-08-08")
            .build(),
            LiteralDTO.builder()
            .with_data_type(data_type=Types.StringType.get())
            .with_value(value="us")
            .build(),
        ]
        expected = IdentityPartitionDTO(
            name=partition_name,
            field_names=field_names,
            values=values,
            properties=properties,
        )
        partition = Partitions.identity(
            name=partition_name,
            field_names=field_names,
            values=[
                Literals.date_literal(date(2025, 8, 8)),
                Literals.string_literal("us"),
            ],
            properties=properties,
        )
        converted = DTOConverters.to_dto(partition)
        self.assertTrue(converted == expected)

    def test_to_dto_list_partition(self):
        partition_name = "p202508_California"
        properties = {"key": "value"}
        partition = Partitions.list(
            partition_name,
            [
                [
                    Literals.date_literal(date(2025, 8, 8)),
                    Literals.string_literal("Los Angeles"),
                ],
                [
                    Literals.date_literal(date(2025, 8, 8)),
                    Literals.string_literal("San Francisco"),
                ],
            ],
            properties,
        )
        expected = ListPartitionDTO(
            name=partition_name,
            lists=[
                [
                    LiteralDTO.builder()
                    .with_data_type(data_type=Types.DateType.get())
                    .with_value(value="2025-08-08")
                    .build(),
                    LiteralDTO.builder()
                    .with_data_type(data_type=Types.StringType.get())
                    .with_value(value="Los Angeles")
                    .build(),
                ],
                [
                    LiteralDTO.builder()
                    .with_data_type(data_type=Types.DateType.get())
                    .with_value(value="2025-08-08")
                    .build(),
                    LiteralDTO.builder()
                    .with_data_type(data_type=Types.StringType.get())
                    .with_value(value="San Francisco")
                    .build(),
                ],
            ],
            properties=properties,
        )
        converted = DTOConverters.to_dto(partition)
        self.assertTrue(converted == expected)
