"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
"""

from abc import ABC, abstractmethod
from typing import List, Dict
from gravitino.api.table import Table
from gravitino.api.rel.column import Column
from gravitino.api.rel.expressions.transforms.transform import Transform
from gravitino.api.rel.expressions.sorts.sort_order import SortOrder
from gravitino.api.rel.expressions.distributions.distribution import Distribution
from gravitino.api.rel.indexes.index import Index
from gravitino.name_identifier import NameIdentifier
from gravitino.namespace import Namespace


class TableCatalog(ABC):
    """
    The TableCatalog class defines the public API for managing tables in a schema.
    If the catalog implementation supports tables, it must implement this interface.
    """

    @abstractmethod
    def list_tables(self, namespace: Namespace) -> List[NameIdentifier]:
        """
        List the tables in a namespace from the catalog.

        Args:
            namespace (Namespace): A namespace.

        Returns:
            List[NameIdentifier]: An array of table identifiers in the namespace.

        Raises:
            NoSuchSchemaException: If the schema does not exist.
        """
        pass

    @abstractmethod
    def load_table(self, ident: NameIdentifier) -> Table:
        """
        Load table metadata by NameIdentifier from the catalog.

        Args:
            ident (NameIdentifier): A table identifier.

        Returns:
            Table: The table metadata.

        Raises:
            NoSuchTableException: If the table does not exist.
        """
        pass

    @abstractmethod
    def create_table(self, ident: NameIdentifier, columns: List[Column], comment: str,
                     properties: Dict[str, str], partitions: List[Transform],
                     distribution: Distribution, sort_orders: List[SortOrder],
                     indexes: List[Index]) -> Table:
        """
        Create a table in the catalog.

        Args:
            ident (NameIdentifier): A table identifier.
            columns (List[Column]): The columns of the new table.
            comment (str): The table comment.
            properties (Dict[str, str]): The table properties.
            partitions (List[Transform]): The table partitioning.
            distribution (Distribution): The distribution of the table.
            sort_orders (List[SortOrder]): The sort orders of the table.
            indexes (List[Index]): The table indexes.

        Returns:
            Table: The created table metadata.

        Raises:
            NoSuchSchemaException: If the schema does not exist.
            TableAlreadyExistsException: If the table already exists.
        """
        pass

    @abstractmethod
    def drop_table(self, ident: NameIdentifier) -> bool:
        """
        Removes both the metadata and the directory associated with the table from the catalog.

        Args:
            ident (NameIdentifier): A table identifier.

        Returns:
            bool: True if the table is dropped, false if the table does not exist.
        """
        pass
