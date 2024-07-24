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
from typing import List, Dict, Optional

from gravitino.api.auditable import Auditable
from gravitino.api.rel.column import Column
from gravitino.api.rel.expressions.transforms.transform import Transform
from gravitino.api.rel.expressions.sorts.sort_order import SortOrder
from gravitino.api.rel.expressions.distributions.distribution import Distribution
from gravitino.api.rel.expressions.distributions.distributions import Distributions
from gravitino.api.rel.indexes.index import Index

class Table(Auditable, ABC):
    """
    An interface representing a table in a namespace. It defines the basic properties of a table.
    A catalog implementation with `TableCatalog` should implement this interface.
    """

    @abstractmethod
    def name(self) -> str:
        """Return the name of the table."""
        pass

    @abstractmethod
    def columns(self) -> List[Column]:
        """Return the columns of the table."""
        pass

    def partitioning(self) -> List[Transform]:
        """Return the physical partitioning of the table."""
        return []

    def sortOrder(self) -> List[SortOrder]:
        """Return the sort order of the table. If no sort order is specified, an empty list is returned."""
        return []

    def distribution(self) -> Distribution:
        """Return the bucketing of the table. If no bucketing is specified, return Distribution.NONE."""
        return Distributions.NONE  # TODO: This would be a default value defined in distributions class

    def index(self) -> List[Index]:
        """Return the indexes of the table. If no indexes are specified, return an empty list."""
        return []

    def comment(self) -> Optional[str]:
        """Return the comment of the table. None is returned if no comment is set."""
        return None

    def properties(self) -> Dict[str, str]:
        """Return the properties of the table. An empty dictionary is returned if no properties are set."""
        return {}

    def support_partitions(self) -> 'SupportsPartitions':
        """
        Table method for working with partitions. If the table does not support partition operations,
        an UnsupportedOperationException is thrown.
        """
        raise NotImplementedError("Table does not support partition operations.")

class SupportsPartitions(ABC):
    """Placeholder for the SupportsPartitions interface."""
    # TODO: move this interface to api/ folder
    pass

