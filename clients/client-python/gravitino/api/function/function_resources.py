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

from typing import List, Optional


class FunctionResources:
    """Represents external resources that are required by a function implementation."""

    def __init__(
        self,
        jars: Optional[List[str]] = None,
        files: Optional[List[str]] = None,
        archives: Optional[List[str]] = None,
    ):
        """Create a FunctionResources instance.

        Args:
            jars: The jar resources.
            files: The file resources.
            archives: The archive resources.
        """
        self._jars = list(jars) if jars else []
        self._files = list(files) if files else []
        self._archives = list(archives) if archives else []

    @classmethod
    def empty(cls) -> "FunctionResources":
        """Returns an empty FunctionResources instance."""
        return cls()

    @classmethod
    def of(
        cls,
        jars: Optional[List[str]] = None,
        files: Optional[List[str]] = None,
        archives: Optional[List[str]] = None,
    ) -> "FunctionResources":
        """Create a FunctionResources instance.

        Args:
            jars: The jar resources.
            files: The file resources.
            archives: The archive resources.

        Returns:
            A FunctionResources instance.
        """
        return cls(jars, files, archives)

    def jars(self) -> List[str]:
        """Returns the jar resources."""
        return list(self._jars)

    def files(self) -> List[str]:
        """Returns the file resources."""
        return list(self._files)

    def archives(self) -> List[str]:
        """Returns the archive resources."""
        return list(self._archives)

    def __eq__(self, other) -> bool:
        if not isinstance(other, FunctionResources):
            return False
        return (
            self._jars == other._jars
            and self._files == other._files
            and self._archives == other._archives
        )

    def __hash__(self) -> int:
        return hash((tuple(self._jars), tuple(self._files), tuple(self._archives)))

    def __repr__(self) -> str:
        return (
            f"FunctionResources(jars={self._jars}, "
            f"files={self._files}, archives={self._archives})"
        )
