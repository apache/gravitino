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
from contextlib import suppress
from typing import Optional

from gravitino.api.rel.column import Column
from gravitino.api.rel.expressions.distributions.distribution import Distribution
from gravitino.api.rel.expressions.sorts.sort_order import SortOrder
from gravitino.api.rel.expressions.transforms.transform import Transform
from gravitino.api.rel.indexes.index import Index
from gravitino.api.rel.table import Table
from gravitino.exceptions.base import (
    NoSuchTableException,
    UnsupportedOperationException,
)
from gravitino.name_identifier import NameIdentifier
from gravitino.namespace import Namespace


class TableCatalog(ABC):
    """The `TableCatalog` interface defines the public API for managing tables in a schema.

    If the catalog implementation supports tables, it must implement this interface.
    """

    @abstractmethod
    def list_tables(self, namespace: Namespace) -> list[NameIdentifier]:
        """List the tables in a namespace from the catalog.

        Args:
            namespace (Namespace): A namespace.

        Returns:
            list[NameIdentifier]: An array of table identifiers in the namespace.

        Raises:
            NoSuchSchemaException: If the schema does not exist.
        """

    @abstractmethod
    def load_table(self, identifier: NameIdentifier) -> Table:
        """Load table metadata by `NameIdentifier` from the catalog.

        Args:
            identifier (NameIdentifier): A table identifier.

        Returns:
            Table: The table metadata.

        Raises:
            NoSuchTableException: If the table does not exist.
        """

    def table_exists(self, identifier: NameIdentifier) -> bool:
        """Check if a table exists using an `NameIdentifier` from the catalog.

        Args:
            identifier (NameIdentifier): A table identifier.

        Returns:
            bool: `True` If the table exists, `False` otherwise.
        """
        with suppress(NoSuchTableException):
            self.load_table(identifier)
            return True
        return False

    @abstractmethod
    def create_table(
        self,
        identifier: NameIdentifier,
        columns: list[Column],
        comment: Optional[str] = None,
        properties: Optional[dict[str, str]] = None,
        partitioning: Optional[list[Transform]] = None,
        distribution: Optional[Distribution] = None,
        sort_orders: Optional[list[SortOrder]] = None,
        indexes: Optional[list[Index]] = None,
    ) -> Table:
        """Create a table in the catalog.

        Args:
            identifier (NameIdentifier):
                A table identifier.
            columns (list[Column]):
                The columns of the new table.
            comment (str, optional):
                The table comment. Defaults to `None`.
            properties (dict[str, str], optional):
                The table properties. Defaults to `None`.
            partitioning (Optional[list[Transform]], optional):
                The table partitioning. Defaults to None.
            distribution (Optional[Distribution], optional):
                The distribution of the table. Defaults to `None`.
            sort_orders (Optional[list[SortOrder]], optional):
                The sort orders of the table. Defaults to `None`.
            indexes (Optional[list[Index]], optional):
                The table indexes. Defaults to `None`.

        Raises:
            NoSuchSchemaException:
                If the schema does not exist.
            TableAlreadyExistsException:
                If the table already exists.

        Returns:
            Table:
                The created table metadata.
        """

    @abstractmethod
    def alter_table(self, identifier: NameIdentifier, *changes) -> Table:
        """Alter a table in the catalog.

        Args:
            identifier (NameIdentifier): A table identifier.
            *changes:
                Table changes (defined in class `TableChange`) to apply to the table.

        Returns:
            Table: The updated table metadata.

        Raises:
            NoSuchTableException:
                If the table does not exist.
            IllegalArgumentException:
                If the change is rejected by the implementation.
        """

    @abstractmethod
    def drop_table(self, identifier: NameIdentifier) -> bool:
        """Drop a table from the catalog.

        Removes both the metadata and the directory associated with the table from the
        file system if the table is not an external table. In case of an external table,
        only the associated metadata is removed.

        Args:
            identifier (NameIdentifier): A table identifier.

        Returns:
            bool: `True` if the table is dropped, `False` if the table does not exist.
        """

    def purge_table(self, identifier: NameIdentifier) -> bool:
        """Drop a table from the catalog and completely remove its data.

        Removes both the metadata and the directory associated with the table completely
        and skipping trash. If the table is an external table or the catalogs don't support
        purge table, `UnsupportedOperationException` is thrown. If the catalog supports to
        purge a table, this method should be overridden. The default implementation throws
        an `UnsupportedOperationException`.

        Args:
            identifier (NameIdentifier): A table identifier.

        Raises:
            UnsupportedOperationException: If the catalog does not support to purge a table.

        Returns:
            bool: `True` if the table is purged, `False` if the table does not exist.
        """
        raise UnsupportedOperationException("purgeTable not supported.")
