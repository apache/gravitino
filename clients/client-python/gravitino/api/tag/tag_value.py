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

from dataclasses import dataclass
from typing import Optional

from gravitino.utils.precondition import Precondition


@dataclass(frozen=True)
class TagValue:
    """A tag name and optional exact value. None matches any assignment."""

    _name: str
    _value: Optional[str] = None

    def __post_init__(self) -> None:
        Precondition.check_argument(
            isinstance(self._name, str) and bool(self._name.strip()),
            "Tag name must not be blank",
        )
        Precondition.check_argument(
            self._value is None
            or (
                isinstance(self._value, str)
                and bool(self._value.strip())
                and len(self._value) <= 256
            ),
            "Tag value must not be blank or exceed 256 characters",
        )

    def name(self) -> str:
        """Return the tag name."""
        return self._name

    def value(self) -> Optional[str]:
        """Return the exact value, or None to match any assignment."""
        return self._value
