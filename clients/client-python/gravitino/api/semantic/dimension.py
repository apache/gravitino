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

from typing import Optional


class Dimension:
    """Marks a Semantic Model field as a dimension."""

    def __init__(self, is_time: Optional[bool] = None):
        self._is_time = is_time

    def is_time(self) -> Optional[bool]:
        """Returns whether the dimension is a time dimension, `None` if not set."""
        return self._is_time

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Dimension):
            return False
        return self._is_time == other.is_time()

    def __hash__(self) -> int:
        return hash(self._is_time)

    def __repr__(self) -> str:
        return f"Dimension(isTime={self._is_time!r})"
