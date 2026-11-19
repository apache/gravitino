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

from typing import Generic, TypeVar

T = TypeVar("T")


class TagList(Generic[T]):
    """A list of visible tags and whether authorization filtering omitted others."""

    def __init__(self, tags: list[T], has_inaccessible_tags: bool):
        self._tags = list(tags)
        self._has_inaccessible_tags = has_inaccessible_tags

    def tags(self) -> list[T]:
        """Return a copy of the visible tag names or tag objects."""
        return list(self._tags)

    def has_inaccessible_tags(self) -> bool:
        """Return whether one or more inaccessible tags were omitted."""
        return self._has_inaccessible_tags
