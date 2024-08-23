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
from typing import List, Tuple

from .expression import Expression

class NamedReference(Expression, ABC):
    """
    Represents a field or column reference in the public logical expression API.
    """

    @staticmethod
    def field(*field_names: str) -> 'FieldReference':
        """
        Returns a FieldReference for the given field name(s). The input can be multiple strings
        for nested fields.
        """
        return FieldReference(field_names)

    @abstractmethod
    def field_name(self) -> Tuple[str, ...]:
        """
        Returns the referenced field name as a tuple of string parts.
        Each string in the tuple represents a part of the field name.
        """
        pass

    def children(self) -> List[Expression]:
        """
        Override from Expression, field references have no children.
        """
        return self.EMPTY_EXPRESSION

    def references(self) -> List['NamedReference']:
        """
        By default, a NamedReference references itself.
        """
        return [self]

class FieldReference(NamedReference):
    def __init__(self, field_name: Tuple[str, ...]):
        self._field_name = field_name

    def field_name(self) -> Tuple[str, ...]:
        return self._field_name

    def __eq__(self, other):
        if isinstance(other, FieldReference):
            return self._field_name == other._field_name
        return False

    def __hash__(self):
        return hash(self._field_name)

    def __str__(self):
        return ".".join(self._field_name)
