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

from abc import ABC, abstractmethod

from gravitino.exceptions.base import IllegalArgumentException


class PolicyAssociationSelector(ABC):
    """Selects which tag assignments a policy association applies to."""

    @abstractmethod
    def type(self) -> str:
        """Returns the selector type."""
        raise NotImplementedError()


class AllValuesSelector(PolicyAssociationSelector):
    """A selector that applies the policy association to all values of the
    tag."""

    TYPE = "ALL_VALUES"

    _INSTANCE = None

    @staticmethod
    def get() -> AllValuesSelector:
        """Returns the singleton all-values selector."""
        if AllValuesSelector._INSTANCE is None:
            AllValuesSelector._INSTANCE = AllValuesSelector()
        return AllValuesSelector._INSTANCE

    def type(self) -> str:
        return self.TYPE

    def __eq__(self, other: object) -> bool:
        return isinstance(other, AllValuesSelector)

    def __hash__(self) -> int:
        return hash(self.TYPE)

    def __repr__(self) -> str:
        return "AllValuesSelector{}"


class TagValueSelector(PolicyAssociationSelector):
    """A selector that applies the policy association to one specific value
    of the tag."""

    TYPE = "TAG_VALUE"

    def __init__(self, value: str):
        if value is None or value.strip() == "":
            raise IllegalArgumentException("tag value cannot be blank")
        self._value = value

    @staticmethod
    def of(value: str) -> TagValueSelector:
        """Creates a selector for the given tag value.

        Args:
            value: The tag value the association applies to.

        Returns:
            TagValueSelector: The selector.
        """
        return TagValueSelector(value)

    def type(self) -> str:
        return self.TYPE

    def value(self) -> str:
        """Returns the tag value this selector applies to."""
        return self._value

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, TagValueSelector):
            return False
        return self._value == other._value

    def __hash__(self) -> int:
        return hash(self._value)

    def __repr__(self) -> str:
        return f"TagValueSelector{{value='{self._value}'}}"
