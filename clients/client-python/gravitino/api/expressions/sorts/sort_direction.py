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

from enum import Enum

from gravitino.api.expressions.sorts.null_ordering import NullOrdering


class SortDirection(Enum):
    ASCENDING = "asc"
    DESCENDING = "desc"

    def __init__(self, value):
        self._default_null_ordering = (
            NullOrdering.NULLS_FIRST if value == "asc" else NullOrdering.NULLS_LAST
        )

    def default_null_ordering(self) -> NullOrdering:
        return self._default_null_ordering

    @staticmethod
    def from_string(s: str) -> "SortDirection":
        if s.lower() == "asc":
            return SortDirection.ASCENDING
        if s.lower() == "desc":
            return SortDirection.DESCENDING
        raise ValueError(f"Unexpected sort direction: {s}")

    def __str__(self):
        return self.value
