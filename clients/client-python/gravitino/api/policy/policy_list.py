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


class PolicyList(Generic[T]):
    """The result of listing policies: the policies plus a flag indicating
    whether the list is partial because some policies are inaccessible to the
    caller."""

    def __init__(self, policies: list[T], has_inaccessible_policies: bool):
        self._policies = list(policies)
        self._has_inaccessible_policies = has_inaccessible_policies

    def policies(self) -> list[T]:
        """Returns the listed policies (a copy)."""
        return list(self._policies)

    def has_inaccessible_policies(self) -> bool:
        """Returns True if some policies were omitted because they are
        inaccessible to the caller."""
        return self._has_inaccessible_policies
