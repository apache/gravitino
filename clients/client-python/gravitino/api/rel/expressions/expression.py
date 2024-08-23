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
from typing import List, Set

class Expression(ABC):
    """
    Base class of the public logical expression API.
    """

    EMPTY_EXPRESSION = []
    """
    `EMPTY_EXPRESSION` is only used as an input when the default `children` method builds the result.
    """
    EMPTY_NAMED_REFERENCE = []
    """
    `EMPTY_NAMED_REFERENCE` is only used as an input when the default `references` method builds
    the result array to avoid repeatedly allocating an empty array.
    """

    @abstractmethod
    def children(self) -> List['Expression']:
        """
        Returns an array of the children of this node. Children should not change.
        """
        pass

    def references(self) -> List['NamedReference']:
        """
        Returns a list of fields or columns that are referenced by this expression.
        This is a default implementation that can be overridden if necessary.
        """
        set_references: Set['NamedReference'] = set()
        for child in self.children():
            set_references.update(child.references())
        return list(set_references)
