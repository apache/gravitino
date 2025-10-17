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
from typing import cast

from gravitino.api.rel.expressions.distributions.distribution import Distribution
from gravitino.api.rel.expressions.distributions.distributions import Distributions
from gravitino.api.rel.expressions.expression import Expression
from gravitino.api.rel.expressions.function_expression import FunctionExpression
from gravitino.api.rel.expressions.literals.literals import Literals
from gravitino.api.rel.expressions.named_reference import NamedReference
from gravitino.api.rel.expressions.sorts.sort_order import SortOrder
from gravitino.api.rel.expressions.sorts.sort_orders import SortOrders
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
from gravitino.dto.rel.sort_order_dto import SortOrderDTO
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
        raise IllegalArgumentException(f"Unsupported expression type: {dto}")

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
