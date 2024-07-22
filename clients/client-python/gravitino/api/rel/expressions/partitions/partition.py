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
from typing import Dict

class Partition(ABC):
    """
    A partition represents a result of partitioning a table. The partition can be either an
    IdentityPartition, ListPartition, or RangePartition, depending on the Table.partitioning() method.

    This interface serves as a contract for all types of partitions, ensuring they provide a common set of methods.
    """

    @abstractmethod
    def name(self) -> str:
        """
        Return the name of the partition.

        Returns:
            str: The name of the partition.
        """
        pass

    @abstractmethod
    def properties(self) -> Dict[str, str]:
        """
        Return the properties of the partition, such as statistics, location, etc.

        Returns:
            Dict[str, str]: A dictionary containing properties of the partition.
        """
        pass
