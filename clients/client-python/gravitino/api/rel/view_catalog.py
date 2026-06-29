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
"""View catalog interface for Gravitino Python client."""

from abc import ABC, abstractmethod
from contextlib import suppress
from typing import Optional

from gravitino.api.rel.view import View
from gravitino.exceptions.base import NoSuchViewException
from gravitino.name_identifier import NameIdentifier
from gravitino.namespace import Namespace


class ViewCatalog(ABC):
    """The `ViewCatalog` interface defines the public API for managing views in a schema.

    If the catalog implementation supports views, it must implement this interface.
    """

    @abstractmethod
    def list_views(self, namespace: Namespace) -> list[NameIdentifier]:
        """List the views in a namespace from the catalog.

        Args:
            namespace (Namespace): A namespace.

        Returns:
            list[NameIdentifier]: An array of view identifiers in the namespace.

        Raises:
            NoSuchSchemaException: If the schema does not exist.
        """

    @abstractmethod
    def load_view(self, identifier: NameIdentifier) -> View:
        """Load view metadata by `NameIdentifier` from the catalog.

        Args:
            identifier (NameIdentifier): A view identifier.

        Returns:
            View: The view metadata.

        Raises:
            NoSuchViewException: If the view does not exist.
        """

    def view_exists(self, identifier: NameIdentifier) -> bool:
        """Check if a view exists using an `NameIdentifier` from the catalog.

        Args:
            identifier (NameIdentifier): A view identifier.

        Returns:
            bool: `True` If the view exists, `False` otherwise.
        """
        with suppress(NoSuchViewException):
            self.load_view(identifier)
            return True
        return False

    @abstractmethod
    def create_view(
        self,
        identifier: NameIdentifier,
        comment: Optional[str] = None,
        properties: Optional[dict[str, str]] = None,
        representations: Optional[list[dict]] = None,
    ) -> View:
        """Create a view in the catalog.

        Args:
            identifier (NameIdentifier): A view identifier.
            comment (Optional[str], optional): The view comment. Defaults to None.
            properties (Optional[dict[str, str]], optional):
                The view properties. Defaults to None.
            view_definition (Optional[str], optional):
                The SQL definition of the view. Defaults to None.

        Raises:
            NoSuchSchemaException: If the schema does not exist.
            ViewAlreadyExistsException: If the view already exists.

        Returns:
            View: The created view metadata.
        """

    @abstractmethod
    def alter_view(self, identifier: NameIdentifier, *changes) -> View:
        """Alter a view in the catalog.

        Args:
            identifier (NameIdentifier): A view identifier.
            *changes: View changes to apply to the view.

        Returns:
            View: The updated view metadata.

        Raises:
            NoSuchViewException: If the view does not exist.
        """

    @abstractmethod
    def drop_view(self, identifier: NameIdentifier) -> bool:
        """Drop a view from the catalog.

        Args:
            identifier (NameIdentifier): A view identifier.

        Returns:
            bool: `True` if the view is dropped, `False` if the view does not exist.
        """