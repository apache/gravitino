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

from abc import abstractmethod
from typing import Optional
from .partition import Partition
from .literal import Literal

class RangePartition(Partition):
    """
    A range partition represents a result of range partitioning. For example, for range partition

    ```
    PARTITION p20200321 VALUES LESS THAN ("2020-03-22")
    ```

    its upper bound is "2020-03-22" and its lower bound is null.

    This class is evolving and may change in future versions.
    """

    @abstractmethod
    def upper(self) -> Optional[Literal]:
        """
        Returns:
            Optional[Literal]: The upper bound of the partition, or None if there is no upper bound.
        """
        pass

    @abstractmethod
    def lower(self) -> Optional[Literal]:
        """
        Returns the lower bound of the partition.

        Returns:
            Optional[Literal]: The lower bound of the partition, or None if there is no lower bound.
        """
        pass
