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
from typing import List

from gravitino.api.rel.expressions.expression import Expression
from gravitino.api.rel.expressions.sorts.sort_direction import SortDirection
from gravitino.api.rel.expressions.sorts.null_ordering import NullOrdering

class SortOrder(Expression, ABC):
    """
    Represents a sort order in the public expression API.
    """

    @abstractmethod
    def expression(self) -> Expression:
        """Return the expression associated with the sort order."""
        pass

    @abstractmethod
    def direction(self) -> SortDirection:
        """Return the direction of the sort order."""
        pass

    @abstractmethod
    def null_ordering(self) -> NullOrdering:
        """Return the null ordering of the sort order."""
        pass

    def children(self) -> List[Expression]:
        """Return a list containing the expression as the only child."""
        return [self.expression()]
