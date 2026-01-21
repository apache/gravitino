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

from abc import ABC, abstractmethod
from typing import List, Optional

from gravitino.api.function.function_column import FunctionColumn
from gravitino.api.function.function_impl import FunctionImpl
from gravitino.api.function.function_param import FunctionParam
from gravitino.api.rel.types.type import Type


class FunctionDefinition(ABC):
    """A function definition that pairs a specific parameter list with its implementations.

    A single function can include multiple definitions (overloads), each with distinct
    parameters, return types, and implementations.

    For scalar or aggregate functions, use return_type() to specify the return type.
    For table-valued functions, use return_columns() to specify the output columns.
    """

    EMPTY_COLUMNS: List[FunctionColumn] = []
    """An empty list of FunctionColumn."""

    @abstractmethod
    def parameters(self) -> List[FunctionParam]:
        """Returns the parameters for this definition.

        Returns:
            The parameters for this definition. May be an empty list for a no-arg definition.
        """
        pass

    def return_type(self) -> Optional[Type]:
        """The return type for scalar or aggregate function definitions.

        Returns:
            The return type, or None if this is a table-valued function definition.
        """
        return None

    def return_columns(self) -> List[FunctionColumn]:
        """The output columns for a table-valued function definition.

        A table-valued function is a function that returns a table instead of a
        scalar value or an aggregate result. The returned table has a fixed schema
        defined by the columns returned from this method.

        Returns:
            The output columns that define the schema of the table returned by
            this definition, or an empty list if this is a scalar or aggregate
            function definition.
        """
        return self.EMPTY_COLUMNS

    @abstractmethod
    def impls(self) -> List[FunctionImpl]:
        """Returns the implementations associated with this definition."""
        pass


class FunctionDefinitions:
    """Factory class for creating FunctionDefinition instances."""

    class SimpleFunctionDefinition(FunctionDefinition):
        """Simple implementation of FunctionDefinition."""

        def __init__(
            self,
            parameters: List[FunctionParam],
            return_type: Optional[Type],
            return_columns: Optional[List[FunctionColumn]],
            impls: List[FunctionImpl],
        ):
            self._parameters = list(parameters) if parameters else []
            self._return_type = return_type
            self._return_columns = (
                list(return_columns)
                if return_columns
                else FunctionDefinition.EMPTY_COLUMNS
            )
            if not impls:
                raise ValueError("Impls cannot be null or empty")
            self._impls = list(impls)

        def parameters(self) -> List[FunctionParam]:
            return list(self._parameters)

        def return_type(self) -> Optional[Type]:
            return self._return_type

        def return_columns(self) -> List[FunctionColumn]:
            return (
                list(self._return_columns)
                if self._return_columns
                else FunctionDefinition.EMPTY_COLUMNS
            )

        def impls(self) -> List[FunctionImpl]:
            return list(self._impls)

        def __eq__(self, other) -> bool:
            if not isinstance(other, FunctionDefinition):
                return False
            return (
                self._parameters == list(other.parameters())
                and self._return_type == other.return_type()
                and self._return_columns == list(other.return_columns())
                and self._impls == list(other.impls())
            )

        def __hash__(self) -> int:
            return hash(
                (
                    tuple(self._parameters),
                    self._return_type,
                    tuple(self._return_columns) if self._return_columns else None,
                    tuple(self._impls),
                )
            )

        def __repr__(self) -> str:
            return (
                f"FunctionDefinition(parameters={self._parameters}, "
                f"returnType={self._return_type}, "
                f"returnColumns={self._return_columns}, "
                f"impls={self._impls})"
            )

    @classmethod
    def of(
        cls,
        parameters: List[FunctionParam],
        return_type: Type,
        impls: List[FunctionImpl],
    ) -> FunctionDefinition:
        """Create a FunctionDefinition instance for a scalar or aggregate function.

        Args:
            parameters: The parameters for this definition, may be empty.
            return_type: The return type for this definition, must not be None.
            impls: The implementations for this definition, must not be empty.

        Returns:
            A FunctionDefinition instance.
        """
        return cls.SimpleFunctionDefinition(parameters, return_type, None, impls)

    @classmethod
    def of_table(
        cls,
        parameters: List[FunctionParam],
        return_columns: List[FunctionColumn],
        impls: List[FunctionImpl],
    ) -> FunctionDefinition:
        """Create a FunctionDefinition instance for a table-valued function.

        Args:
            parameters: The parameters for this definition, may be empty.
            return_columns: The return columns for this definition, must not be empty.
            impls: The implementations for this definition, must not be empty.

        Returns:
            A FunctionDefinition instance.
        """
        return cls.SimpleFunctionDefinition(parameters, None, return_columns, impls)
