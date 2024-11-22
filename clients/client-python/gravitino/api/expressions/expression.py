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

from __future__ import annotations
from abc import ABC
from typing import List, Set, TYPE_CHECKING

if TYPE_CHECKING:
    from gravitino.api.expressions.named_reference import NamedReference


class Expression(ABC):
    """Base class of the public logical expression API."""

    EMPTY_EXPRESSION: List[Expression] = []

    def children(self) -> List[Expression]:
        """Returns a list of the children of this node. Children should not change."""
        return self.EMPTY_EXPRESSION

    def references(self) -> List[NamedReference]:
        """Returns a list of fields or columns that are referenced by this expression."""

        ref_set: Set[NamedReference] = set()
        for child in self.children():
            ref_set.update(child.references())
        return list(ref_set)
