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

from abc import abstractmethod
from typing import Optional

from gravitino.api.auditable import Auditable
from gravitino.api.rel.column import Column
from gravitino.api.rel.representation import Representation
from gravitino.api.rel.sql_representation import SQLRepresentation
from gravitino.api.tag.supports_tags import SupportsTags
from gravitino.exceptions.base import UnsupportedOperationException


class View(Auditable):
    """An interface representing a logical view in a namespace."""

    @abstractmethod
    def name(self) -> str:
        """Returns the view name."""

    def comment(self) -> Optional[str]:
        """Returns the view comment."""
        return None

    @abstractmethod
    def columns(self) -> list[Column]:
        """Returns the output columns of the view."""

    @abstractmethod
    def representations(self) -> list[Representation]:
        """Returns the representations of the view."""

    def default_catalog(self) -> Optional[str]:
        """Returns the default catalog used to resolve unqualified identifiers."""
        return None

    def default_schema(self) -> Optional[str]:
        """Returns the default schema used to resolve unqualified identifiers."""
        return None

    def sql_for(self, dialect: Optional[str]) -> Optional[SQLRepresentation]:
        """Returns the SQL representation for the given dialect."""
        if dialect is None:
            return None
        for representation in self.representations():
            if (
                isinstance(representation, SQLRepresentation)
                and representation.dialect().lower() == dialect.lower()
            ):
                return representation
        return None

    def properties(self) -> dict[str, str]:
        """Returns the view properties."""
        return {}

    def supports_tags(self) -> SupportsTags:
        """Return tag operations if the view supports tags.

        Raises:
            UnsupportedOperationException: If the view does not support tag operations.

        Returns:
            SupportsTags: The tag operations supported by the view.
        """
        raise UnsupportedOperationException("View does not support tag operations.")
