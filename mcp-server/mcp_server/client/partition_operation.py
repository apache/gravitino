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


class PartitionOperation(ABC):
    """
    Abstract base class for Gravitino partition operations.
    """

    @abstractmethod
    async def list_of_partitions(
        self,
        catalog_name: str,
        schema_name: str,
        table_name: str,
        details: bool = False,
    ) -> str:
        """
        Retrieve the partitions of a specific table.

        Args:
            catalog_name: Name of the catalog
            schema_name: Name of the schema
            table_name: Name of the table
            details: When False, return partition names only. When True, return
                full partition metadata.

        Returns:
            str: JSON-formatted string containing partition names or partition
                metadata
        """
        pass

    @abstractmethod
    async def get_partition(
        self,
        catalog_name: str,
        schema_name: str,
        table_name: str,
        partition_name: str,
    ) -> str:
        """
        Load detailed information of a specific partition.

        Args:
            catalog_name: Name of the catalog
            schema_name: Name of the schema
            table_name: Name of the table
            partition_name: Name of the partition

        Returns:
            str: JSON-formatted string containing full partition metadata
        """
        pass
