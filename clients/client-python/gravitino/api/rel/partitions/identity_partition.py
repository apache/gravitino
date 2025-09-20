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
from typing import Any, List

from gravitino.api.expressions.literals.literal import Literal
from gravitino.api.rel.partitions.partition import Partition


class IdentityPartition(Partition):
    """
    An identity partition represents a result of identity partitioning. For example, for Hive
    partition

    ```
    PARTITION (dt='2008-08-08',country='us')
    ```

    its partition name is "dt=2008-08-08/country=us", field names are [["dt"], ["country"]] and
    values are ["2008-08-08", "us"].

    APIs that are still evolving towards becoming stable APIs, and can change from one feature release to another (0.5.0 to 0.6.0).
    """

    @abstractmethod
    def field_names(self) -> List[List[str]]:
        """
        Returns:
            List[List[str]]: A list of lists representing the field names of the identity partition.
        """
        pass

    @abstractmethod
    def values(self) -> List[Literal[Any]]:
        """
        Returns:
            List[Literal[Any]]: The values of the identity partition.
        """
        pass
