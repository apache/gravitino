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


class StatisticOperation(ABC):
    """
    Abstract base class for Gravitino statistic operations.
    """

    @abstractmethod
    async def list_of_statistics(
        self, metalake_name: str, metadata_type: str, metadata_fullname: str
    ) -> str:
        """
        Retrieve the list of statistics for a specific metadata type and fullname within a metalake.
        Args:
            metalake_name: Name of the metalake
            metadata_type: Type of metadata (e.g., table, column)
            metadata_fullname: Full name of the metadata item

        Returns:
            str: JSON-formatted string containing statistics information
        """
        pass

    # pylint: disable=R0917
    @abstractmethod
    async def list_statistic_for_partition(
        self,
        metalake_name: str,
        metadata_type: str,
        metadata_fullname: str,
        from_partition_name: str,
        to_partition_name: str,
        from_inclusive: bool = True,
        to_inclusive: bool = False,
    ) -> str:
        """
        Retrieve statistics for a specific partition range of a metadata item.
        Note: This method is currently only supported list statistics for partitions of tables.
            So `metadata_type` should always be "table".

        Args:
            metalake_name: Name of the metalake
            metadata_type: Type of metadata, should be "table" for partition statistics
            metadata_fullname: Full name of the metadata item, the format should be
                "{catalog}.{schema}.{table}".
            from_partition_name: Starting partition name
            to_partition_name: Ending partition name
            from_inclusive: Whether to include the starting partition
            to_inclusive: Whether to include the ending partition

        Returns:
            str: JSON-formatted string containing statistics for the specified partitions
        """
        pass
