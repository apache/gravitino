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
"""View interface for Gravitino Python client."""

from abc import ABC, abstractmethod
from typing import Optional


class View(ABC):
    """The `View` interface defines the metadata of a view."""

    @abstractmethod
    def name(self) -> str:
        """Get the name of the view.

        Returns:
            str: The view name.
        """

    @abstractmethod
    def comment(self) -> Optional[str]:
        """Get the comment of the view.

        Returns:
            Optional[str]: The view comment.
        """

    @abstractmethod
    def properties(self) -> dict[str, str]:
        """Get the properties of the view.

        Returns:
            dict[str, str]: The view properties.
        """

    @abstractmethod
    def view_definition(self) -> Optional[str]:
        """Get the SQL definition of the view.

        Returns:
            Optional[str]: The view definition.
        """