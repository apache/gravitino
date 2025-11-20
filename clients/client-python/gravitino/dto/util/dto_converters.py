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

from functools import singledispatchmethod
from typing import cast, overload

from gravitino.api.rel.column import Column
from gravitino.api.rel.expressions.distributions.distribution import Distribution
from gravitino.api.rel.expressions.distributions.distributions import Distributions
from gravitino.api.rel.expressions.expression import Expression
from gravitino.api.rel.expressions.function_expression import FunctionExpression
from gravitino.api.rel.expressions.literals.literal import Literal
from gravitino.api.rel.expressions.literals.literals import Literals
from gravitino.api.rel.expressions.named_reference import FieldReference, NamedReference
from gravitino.api.rel.expressions.sorts.sort_order import SortOrder
from gravitino.api.rel.expressions.sorts.sort_orders import SortOrders
from gravitino.api.rel.expressions.transforms.transform import Transform
from gravitino.api.rel.expressions.transforms.transforms import Transforms
from gravitino.api.rel.expressions.unparsed_expression import UnparsedExpression
from gravitino.api.rel.indexes.index import Index
from gravitino.api.rel.indexes.indexes import Indexes
from gravitino.api.rel.partitions.identity_partition import IdentityPartition
from gravitino.api.rel.partitions.list_partition import ListPartition
from gravitino.api.rel.partitions.partition import Partition
from gravitino.api.rel.partitions.range_partition import RangePartition
from gravitino.api.rel.table import Table
from gravitino.api.rel.types.types import Types
from gravitino.client.relational_table import RelationalTable
from gravitino.dto.rel.column_dto import ColumnDTO
from gravitino.dto.rel.distribution_dto import DistributionDTO
from gravitino.dto.rel.expressions.field_reference_dto import FieldReferenceDTO
from gravitino.dto.rel.expressions.func_expression_dto import FuncExpressionDTO
from gravitino.dto.rel.expressions.function_arg import FunctionArg
from gravitino.dto.rel.expressions.literal_dto import LiteralDTO
from gravitino.dto.rel.expressions.unparsed_expression_dto import UnparsedExpressionDTO
from gravitino.dto.rel.indexes.index_dto import IndexDTO
from gravitino.dto.rel.partitioning.bucket_partitioning_dto import BucketPartitioningDTO
from gravitino.dto.rel.partitioning.function_partitioning_dto import (
    FunctionPartitioningDTO,
)
from gravitino.dto.rel.partitioning.list_partitioning_dto import ListPartitioningDTO
from gravitino.dto.rel.partitioning.partitioning import (
    Partitioning,
    SingleFieldPartitioning,
)
from gravitino.dto.rel.partitioning.range_partitioning_dto import RangePartitioningDTO
from gravitino.dto.rel.partitioning.truncate_partitioning_dto import (
    TruncatePartitioningDTO,
)
from gravitino.dto.rel.partitions.identity_partition_dto import IdentityPartitionDTO
from gravitino.dto.rel.partitions.list_partition_dto import ListPartitionDTO
from gravitino.dto.rel.partitions.partition_dto import PartitionDTO
from gravitino.dto.rel.partitions.range_partition_dto import RangePartitionDTO
from gravitino.dto.rel.sort_order_dto import SortOrderDTO
from gravitino.dto.rel.table_dto import TableDTO
from gravitino.exceptions.base import IllegalArgumentException


class DTOConverters:
    """Utility class for converting between DTOs and domain objects."""

    @staticmethod
    def from_function_arg(arg: FunctionArg) -> Expression:
        """Converts a FunctionArg DTO to an Expression.

        Args:
            arg (FunctionArg): The function argument DTO to be converted.

        Returns:
            Expression: The expression.
        """
        arg_type = arg.arg_type()
        if arg_type is FunctionArg.ArgType.LITERAL:
            dto = cast(LiteralDTO, arg)
            if dto.value() is None or dto.data_type() == Types.NullType.get():
                return Literals.NULL
            return Literals.of(dto.value(), dto.data_type())
        if arg_type is FunctionArg.ArgType.FIELD:
            dto = cast(FieldReferenceDTO, arg)
            return NamedReference.field(dto.field_name())
        if arg_type is FunctionArg.ArgType.FUNCTION:
            dto = cast(FuncExpressionDTO, arg)
            return FunctionExpression.of(
                dto.function_name(), *DTOConverters.from_function_args(dto.args())
            )
        if arg_type is FunctionArg.ArgType.UNPARSED:
            dto = cast(UnparsedExpressionDTO, arg)
            return UnparsedExpression.of(dto.unparsed_expression())
        raise IllegalArgumentException(f"Unsupported expression type: {arg}")

    @staticmethod
    def from_function_args(args: list[FunctionArg]) -> list[Expression]:
        """Converts a FunctionArg DTO to an Expression.

        Args:
            args (list[FunctionArg]): The function argument DTOs to be converted.

        Returns:
            list[Expression]: The list of expressions.
        """
        if not args:
            return Expression.EMPTY_EXPRESSION
        return [DTOConverters.from_function_arg(arg) for arg in args]

    @singledispatchmethod
    @staticmethod
    def from_dto(dto) -> object:
        raise IllegalArgumentException(f"Unsupported DTO type: {type(dto)}")

    @from_dto.register
    @staticmethod
    def _(dto: DistributionDTO) -> Distribution:
        """Converts a DistributionDTO to a Distribution.

        Args:
            dto (DistributionDTO): The distribution DTO.

        Returns:
            Distribution: The distribution.
        """
        if dto is None or DistributionDTO.NONE.equals(dto):
            return Distributions.NONE

        return Distributions.of(
            dto.strategy(),
            dto.number(),
            *DTOConverters.from_function_args(dto.args()),
        )

    @from_dto.register
    @staticmethod
    def _(dto: IndexDTO) -> Index:
        """Converts an IndexDTO to an Index.

        Args:
            dto (IndexDTO): The Index DTO to be converted.

        Returns:
            Index: The index.
        """
        return Indexes.of(dto.type(), dto.name(), dto.field_names())

    @from_dto.register
    @staticmethod
    def _(dto: SortOrderDTO) -> SortOrder:
        """Converts a SortOrderDTO to a SortOrder.

        Args:
            dto (SortOrderDTO): The sort order DTO to be converted.

        Returns:
            SortOrder: The sort order.
        """
        return SortOrders.of(
            DTOConverters.from_function_arg(dto.sort_term()),
            dto.direction(),
            dto.null_ordering(),
        )

    @from_dto.register
    @staticmethod
    def _(dto: ColumnDTO) -> Column:
        """Converts a ColumnDTO to a Column.

        Args:
            dto (ColumnDTO): The column DTO to be converted.

        Returns:
            Column: The column.
        """
        if dto.default_value() == Column.DEFAULT_VALUE_NOT_SET:
            return dto
        return Column.of(
            dto.name(),
            dto.data_type(),
            dto.comment(),
            dto.nullable(),
            dto.auto_increment(),
            DTOConverters.from_function_arg(dto.default_value()),
        )

    @from_dto.register
    @staticmethod
    def _(dto: Partitioning) -> Transform:  # pylint: disable=too-many-return-statements
        """Converts a partitioning DTO to a Transform.

        Args:
            dto (Partitioning): The partitioning DTO to be converted.

        Returns:
            Transform: The transform.
        """
        strategy = dto.strategy()
        if strategy is Partitioning.Strategy.IDENTITY:
            return Transforms.identity(cast(SingleFieldPartitioning, dto).field_name())
        if strategy is Partitioning.Strategy.YEAR:
            return Transforms.year(cast(SingleFieldPartitioning, dto).field_name())
        if strategy is Partitioning.Strategy.MONTH:
            return Transforms.month(cast(SingleFieldPartitioning, dto).field_name())
        if strategy is Partitioning.Strategy.DAY:
            return Transforms.day(cast(SingleFieldPartitioning, dto).field_name())
        if strategy is Partitioning.Strategy.HOUR:
            return Transforms.hour(cast(SingleFieldPartitioning, dto).field_name())
        if strategy is Partitioning.Strategy.BUCKET:
            bucket_partitioning_dto = cast(BucketPartitioningDTO, dto)
            return Transforms.bucket(
                bucket_partitioning_dto.num_buckets(),
                *bucket_partitioning_dto.field_names(),
            )
        if strategy is Partitioning.Strategy.TRUNCATE:
            truncate_partitioning_dto = cast(TruncatePartitioningDTO, dto)
            return Transforms.truncate(
                truncate_partitioning_dto.width(),
                truncate_partitioning_dto.field_name(),
            )
        if strategy is Partitioning.Strategy.LIST:
            list_partitioning_dto = cast(ListPartitioningDTO, dto)
            return Transforms.list(
                field_names=list_partitioning_dto.field_names(),
                assignments=[
                    cast(ListPartition, DTOConverters.from_dto(p))
                    for p in list_partitioning_dto.assignments()
                ],
            )
        if strategy is Partitioning.Strategy.RANGE:
            range_partitioning_dto = cast(RangePartitioningDTO, dto)
            return Transforms.range(
                range_partitioning_dto.field_name(),
                [
                    cast(RangePartition, DTOConverters.from_dto(p))
                    for p in range_partitioning_dto.assignments()
                ],
            )
        if strategy is Partitioning.Strategy.FUNCTION:
            function_partitioning_dto = cast(FunctionPartitioningDTO, dto)
            return Transforms.apply(
                function_partitioning_dto.function_name(),
                DTOConverters.from_function_args(function_partitioning_dto.args()),
            )
        raise IllegalArgumentException(f"Unsupported partitioning: {strategy}")

    @from_dto.register
    @staticmethod
    def _(dto: TableDTO) -> Table:
        """Converts a TableDTO to a Table.

        Args:
            dto (TableDTO): The table DTO to be converted.

        Returns:
            Table: The table.
        """

        return RelationalTable(
            name=dto.name(),
            columns=DTOConverters.from_dtos(dto.columns()),
            partitioning=DTOConverters.from_dtos(dto.partitioning()),
            sort_order=DTOConverters.from_dtos(dto.sort_order()),
            distribution=DTOConverters.from_dto(dto.distribution()),
            index=DTOConverters.from_dtos(dto.index()),
            comment=dto.comment(),
            properties=dto.properties(),
            audit_info=dto.audit_info(),
        )

    @overload
    @staticmethod
    def from_dtos(dtos: list[ColumnDTO]) -> list[Column]: ...

    @overload
    @staticmethod
    def from_dtos(dtos: list[IndexDTO]) -> list[Index]: ...

    @overload
    @staticmethod
    def from_dtos(dtos: list[SortOrderDTO]) -> list[SortOrder]: ...

    @overload
    @staticmethod
    def from_dtos(dtos: list[Partitioning]) -> list[Transform]: ...

    @staticmethod
    def from_dtos(dtos):
        """Converts list of `ColumnDTO`, `IndexDTO`, `SortOrderDTO`, or `Partitioning`
        to the corresponding list of `Column`s, `Index`es, `SortOrder`s, or `Transform`s.

        Args:
            dtos (list[ColumnDTO] | list[IndexDTO] | list[SortOrderDTO] | list[Partitioning]):
                The DTOs to be converted.

        Returns:
            list[Column] | list[Index] | list[SortOrder] | list[Transform]:
                The list of Columns, Indexes, SortOrders, or Transforms depends on the input DTOs.
        """
        if not dtos:
            return []
        return [DTOConverters.from_dto(dto) for dto in dtos]

    @staticmethod
    def to_function_arg(expression: Expression) -> FunctionArg:
        """Converts an Expression to an FunctionArg DTO.

        Args:
            expression (Expression): The expression to be converted.

        Returns:
            FunctionArg: The expression DTO.
        """

        if isinstance(expression, FunctionArg):
            return cast(FunctionArg, expression)
        if isinstance(expression, Literal):
            if expression is Literals.NULL:
                return LiteralDTO.NULL
            return (
                LiteralDTO.builder()
                .with_value(str(expression.value()))
                .with_data_type(expression.data_type())
                .build()
            )
        if isinstance(expression, FieldReference):
            return (
                FieldReferenceDTO.builder()
                .with_field_name(expression.field_name())
                .build()
            )
        if isinstance(expression, FunctionExpression):
            return (
                FuncExpressionDTO.builder()
                .with_function_name(expression.function_name())
                .with_function_args(
                    [
                        DTOConverters.to_function_arg(arg)
                        for arg in expression.arguments()
                    ]
                )
                .build()
            )
        if isinstance(expression, UnparsedExpression):
            return (
                UnparsedExpressionDTO.builder()
                .with_unparsed_expression(expression.unparsed_expression())
                .build()
            )
        raise IllegalArgumentException(
            f"Unsupported expression type: {expression.__class__.__name__}"
        )

    @singledispatchmethod
    @staticmethod
    def to_dto(obj) -> object:
        raise IllegalArgumentException(f"Unsupported type: {type(obj)}")

    @to_dto.register
    @staticmethod
    def _(obj: Partition) -> PartitionDTO:
        if isinstance(obj, RangePartition):
            range_partition = cast(RangePartition, obj)
            return RangePartitionDTO(
                name=range_partition.name(),
                upper=cast(
                    LiteralDTO, DTOConverters.to_function_arg(range_partition.upper())
                ),
                lower=cast(
                    LiteralDTO, DTOConverters.to_function_arg(range_partition.lower())
                ),
                properties=range_partition.properties(),
            )
        if isinstance(obj, IdentityPartition):
            identity_partition = cast(IdentityPartition, obj)
            return IdentityPartitionDTO(
                name=identity_partition.name(),
                values=[
                    cast(LiteralDTO, DTOConverters.to_function_arg(v))
                    for v in identity_partition.values()
                ],
                field_names=identity_partition.field_names(),
                properties=identity_partition.properties(),
            )
        if isinstance(obj, ListPartition):
            list_partition = cast(ListPartition, obj)
            return ListPartitionDTO(
                name=list_partition.name(),
                lists=[
                    [
                        cast(LiteralDTO, DTOConverters.to_function_arg(v))
                        for v in list_item
                    ]
                    for list_item in list_partition.lists()
                ],
                properties=list_partition.properties(),
            )
        raise IllegalArgumentException(
            f"Unsupported partition type: {obj.__class__.__name__}"
        )
