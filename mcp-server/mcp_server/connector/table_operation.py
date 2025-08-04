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


class TableOperation(ABC):
    """
    Abstract base class for Gravitino table operations.
    """

    @abstractmethod
    async def get_list_of_tables(
        self, catalog_name: str, schema_name: str
    ) -> str:
        """
        Retrieve the list of tables within a specified schema and catalog.

        Args:
            catalog_name: Name of the catalog
            schema_name: Name of the schema

        Returns:
            str: JSON-formatted string containing table list information
        """
        pass

    @abstractmethod
    async def load_table(
        self, catalog_name: str, schema_name: str, table_name: str
    ) -> str:
        """
        Load detailed information of a specific table.

        Args:
            catalog_name: Name of the catalog
            schema_name: Name of the schema
            table_name: Name of the table

        Returns:
            str: JSON-formatted string containing full table metadata
        """
        pass
