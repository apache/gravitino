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
from typing import Dict


class Partition(ABC):
    """
    A partition represents a result of partitioning a table. The partition can be either a
    `IdentityPartition`, `ListPartition`, or `RangePartition`. It depends on the `Table.partitioning()`.

    APIs that are still evolving towards becoming stable APIs, and can change from one feature release to another (0.5.0 to 0.6.0).
    """

    @abstractmethod
    def name(self) -> str:
        """
        Returns:
            str: The name of the partition.
        """
        pass

    @abstractmethod
    def properties(self) -> Dict[str, str]:
        """
        Returns:
            Dict[str, str]: The properties of the partition, such as statistics, location, etc.
        """
        pass
