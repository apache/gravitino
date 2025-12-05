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

from gravitino.api.auditable import Auditable
from gravitino.api.rel.column import Column
from gravitino.api.rel.expressions.distributions.distribution import Distribution
from gravitino.api.rel.expressions.distributions.distributions import Distributions
from gravitino.api.rel.expressions.sorts.sort_order import SortOrder
from gravitino.api.rel.expressions.transforms.transform import Transform
from gravitino.api.rel.expressions.transforms.transforms import Transforms
from gravitino.api.rel.indexes.index import Index
from gravitino.api.rel.indexes.indexes import Indexes


class Table(Auditable):
    """An interface representing a table in a `Namespace`.

    It defines the basic properties of a table. A catalog implementation with `TableCatalog`
    should implement this interface.
    """

    @abstractmethod
    def name(self) -> str:
        """Gets name of the table.

        Returns:
            str: Name of the table.
        """

    @abstractmethod
    def columns(self) -> list[Column]:
        """Gets the columns of the table.

        Returns:
            list[Column]: The columns of the table.
        """

    def partitioning(self) -> list[Transform]:
        """Gets the physical partitioning of the table.

        Returns:
            list[Transform]: The physical partitioning of the table.
        """

        return Transforms.EMPTY_TRANSFORM

    def sort_order(self) -> list[SortOrder]:
        """Gets the sort order of the table.

        Returns:
            list[SortOrder]:
                The sort order of the table. If no sort order is specified, an empty list is returned.
        """

        return []

    def distribution(self) -> Distribution:
        """Gets the bucketing of the table.

        Returns:
            Distribution:
                The bucketing of the table. If no bucketing is specified, `Distribution.NONE` is returned.
        """

        return Distributions.NONE

    def index(self) -> list[Index]:
        """Gets the indexes of the table.

        Returns:
            list[Index]:
                The indexes of the table. If no indexes are specified, `Indexes.EMPTY_INDEXES` is returned.
        """

        return Indexes.EMPTY_INDEXES

    def comment(self) -> Optional[str]:
        """Gets the comment of the table.

        Returns:
            str (optional):
                The comment of the table. `None` is returned if no comment is set.
        """

        return None

    def properties(self) -> dict[str, str]:
        """Gets the properties of the table.

        Returns:
            dict[str, str]:
                The properties of the table. Empty dictionary is returned if no properties are set.
        """

        return {}
