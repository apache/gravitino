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

from gravitino.api.function.function import Function
from gravitino.api.function.function_change import FunctionChange
from gravitino.api.function.function_definition import FunctionDefinition
from gravitino.api.function.function_type import FunctionType
from gravitino.exceptions.base import NoSuchFunctionException
from gravitino.name_identifier import NameIdentifier
from gravitino.namespace import Namespace


class FunctionCatalog(ABC):
    """The FunctionCatalog interface defines the public API for managing functions in a schema."""

    @abstractmethod
    def list_functions(self, namespace: Namespace) -> List[NameIdentifier]:
        """List the functions in a namespace from the catalog.

        Args:
            namespace: A namespace.

        Returns:
            A list of function identifiers in the namespace.

        Raises:
            NoSuchSchemaException: If the schema does not exist.
        """
        pass

    @abstractmethod
    def list_function_infos(self, namespace: Namespace) -> List[Function]:
        """List the functions with details in a namespace from the catalog.

        Args:
            namespace: A namespace.

        Returns:
            A list of functions in the namespace.

        Raises:
            NoSuchSchemaException: If the schema does not exist.
        """
        pass

    @abstractmethod
    def get_function(self, ident: NameIdentifier) -> Function:
        """Get a function by NameIdentifier from the catalog.

        The identifier only contains the schema and function name. A function may
        include multiple definitions (overloads) in the result.

        Args:
            ident: A function identifier.

        Returns:
            The function with the given name.

        Raises:
            NoSuchFunctionException: If the function does not exist.
        """
        pass

    def function_exists(self, ident: NameIdentifier) -> bool:
        """Check if a function with the given name exists in the catalog.

        Args:
            ident: The function identifier.

        Returns:
            True if the function exists, False otherwise.
        """
        try:
            self.get_function(ident)
            return True
        except NoSuchFunctionException:
            return False

    @abstractmethod
    def register_function(
        self,
        ident: NameIdentifier,
        comment: Optional[str],
        function_type: FunctionType,
        deterministic: bool,
        definitions: List[FunctionDefinition],
    ) -> Function:
        """Register a function with one or more definitions (overloads).

        Each definition contains its own return type (for scalar/aggregate functions)
        or return columns (for table-valued functions).

        Args:
            ident: The function identifier.
            comment: The optional function comment.
            function_type: The function type (SCALAR, AGGREGATE, or TABLE).
            deterministic: Whether the function is deterministic.
            definitions: The function definitions, each containing parameters,
                return type/columns, and implementations.

        Returns:
            The registered function.

        Raises:
            NoSuchSchemaException: If the schema does not exist.
            FunctionAlreadyExistsException: If the function already exists.
        """
        pass

    @abstractmethod
    def alter_function(
        self, ident: NameIdentifier, *changes: FunctionChange
    ) -> Function:
        """Applies FunctionChange changes to a function in the catalog.

        Implementations may reject the changes. If any change is rejected, no changes
        should be applied to the function.

        Args:
            ident: The NameIdentifier instance of the function to alter.
            changes: The FunctionChange instances to apply to the function.

        Returns:
            The updated Function instance.

        Raises:
            NoSuchFunctionException: If the function does not exist.
            IllegalArgumentException: If the change is rejected by the implementation.
        """
        pass

    @abstractmethod
    def drop_function(self, ident: NameIdentifier) -> bool:
        """Drop a function by name.

        Args:
            ident: The name identifier of the function.

        Returns:
            True if the function is deleted, False if the function does not exist.
        """
        pass
