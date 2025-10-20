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
from typing import Optional, Union, cast

from gravitino.api.audit import Audit
from gravitino.api.rel.column import Column
from gravitino.api.rel.expressions.distributions.distribution import Distribution
from gravitino.api.rel.expressions.distributions.distributions import Distributions
from gravitino.api.rel.expressions.expression import Expression
from gravitino.api.rel.expressions.function_expression import FunctionExpression
from gravitino.api.rel.expressions.literals.literals import Literals
from gravitino.api.rel.expressions.named_reference import NamedReference
from gravitino.api.rel.expressions.sorts.sort_order import SortOrder
from gravitino.api.rel.expressions.sorts.sort_orders import SortOrders
from gravitino.api.rel.expressions.transforms.transform import Transform
from gravitino.api.rel.expressions.transforms.transforms import Transforms
from gravitino.api.rel.expressions.unparsed_expression import UnparsedExpression
from gravitino.api.rel.indexes.index import Index
from gravitino.api.rel.indexes.indexes import Indexes
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
from gravitino.dto.rel.partitioning.partitioning import Partitioning
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
            return Transforms.identity(dto.field_name())
        if strategy is Partitioning.Strategy.YEAR:
            return Transforms.year(dto.field_name())
        if strategy is Partitioning.Strategy.MONTH:
            return Transforms.month(dto.field_name())
        if strategy is Partitioning.Strategy.DAY:
            return Transforms.day(dto.field_name())
        if strategy is Partitioning.Strategy.HOUR:
            return Transforms.hour(dto.field_name())
        if strategy is Partitioning.Strategy.BUCKET:
            return Transforms.bucket(dto.num_buckets(), *dto.field_names())
        if strategy is Partitioning.Strategy.TRUNCATE:
            return Transforms.truncate(dto.width(), dto.field_name())
        if strategy is Partitioning.Strategy.LIST:
            return Transforms.list(
                field_names=dto.field_names(),
                assignments=[DTOConverters.from_dto(p) for p in dto.assignments()],
            )
        if strategy is Partitioning.Strategy.RANGE:
            return Transforms.range(
                dto.field_name(),
                [DTOConverters.from_dto(p) for p in dto.assignments()],
            )
        if strategy is Partitioning.Strategy.FUNCTION:
            return Transforms.apply(
                dto.function_name(), DTOConverters.from_function_args(dto.args())
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

        class TableImpl(Table):  # pylint: disable=too-many-instance-attributes
            """A table implementation."""

            def __init__(
                self,
                name: str,
                columns: list[Column],
                partitioning: list[Transform],
                sort_order: list[SortOrder],
                distribution: Distribution,
                index: list[Index],
                comment: Optional[str],
                properties: dict[str, str],
                audit_info: Audit,
            ):
                self._name = name
                self._columns = columns
                self._partitioning = partitioning
                self._sort_order = sort_order
                self._distribution = distribution
                self._index = index
                self._comment = comment
                self._properties = properties
                self._audit_info = audit_info

            def name(self) -> str:
                return self._name

            def columns(self) -> list[Column]:
                return self._columns

            def partitioning(self) -> list[Transform]:
                return self._partitioning

            def sort_order(self) -> list[SortOrder]:
                return self._sort_order

            def distribution(self) -> Distribution:
                return self._distribution

            def index(self) -> list[Index]:
                return self._index

            def comment(self) -> Optional[str]:
                return self._comment

            def properties(self) -> dict[str, str]:
                return self._properties

            def audit_info(self) -> Audit:
                return self._audit_info

        return TableImpl(
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

    @staticmethod
    def from_dtos(
        dtos: list[Union[ColumnDTO, IndexDTO, SortOrderDTO, Partitioning]],
    ) -> list[Union[Column, Index, SortOrder, Transform]]:
        """Converts list of `ColumnDTO`, `IndexDTO`, `SortOrderDTO`, or `Partitioning`
        to the corresponding list of `Column`s, `Index`es, `SortOrder`s, or `Transform`s.

        Args:
            dtos (list[Union[ColumnDTO, IndexDTO, SortOrderDTO, Partitioning]]):
                The DTOs to be converted.

        Returns:
            list[Union[Column, Index, SortOrder, Transform]]: The list of Indexes.
        """
        if not dtos:
            return []
        return [DTOConverters.from_dto(dto) for dto in dtos]
