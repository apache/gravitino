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

from dataclasses import dataclass, field
from typing import List, Optional

from dataclasses_json import DataClassJsonMixin, config
from gravitino.api.function.function_resources import FunctionResources


@dataclass
class FunctionResourcesDTO(DataClassJsonMixin):
    """DTO for function resources."""

    _jars: Optional[List[str]] = field(default=None, metadata=config(field_name="jars"))
    _files: Optional[List[str]] = field(
        default=None, metadata=config(field_name="files")
    )
    _archives: Optional[List[str]] = field(
        default=None, metadata=config(field_name="archives")
    )

    def jars(self) -> List[str]:
        """Returns the jar resources."""
        return list(self._jars) if self._jars else []

    def files(self) -> List[str]:
        """Returns the file resources."""
        return list(self._files) if self._files else []

    def archives(self) -> List[str]:
        """Returns the archive resources."""
        return list(self._archives) if self._archives else []

    def to_function_resources(self):
        """Convert this DTO to a FunctionResources instance."""
        return FunctionResources.of(self._jars, self._files, self._archives)

    @classmethod
    def from_function_resources(cls, resources) -> "FunctionResourcesDTO":
        """Create a FunctionResourcesDTO from a FunctionResources instance."""
        if resources is None:
            return None
        return cls(
            _jars=resources.jars() if resources.jars() else None,
            _files=resources.files() if resources.files() else None,
            _archives=resources.archives() if resources.archives() else None,
        )

    def __eq__(self, other) -> bool:
        if not isinstance(other, FunctionResourcesDTO):
            return False
        return (
            self._jars == other._jars
            and self._files == other._files
            and self._archives == other._archives
        )

    def __hash__(self) -> int:
        return hash(
            (
                tuple(self._jars) if self._jars else None,
                tuple(self._files) if self._files else None,
                tuple(self._archives) if self._archives else None,
            )
        )
