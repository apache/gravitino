"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

from abc import abstractmethod
from typing import Optional, Dict

from gravitino.api.auditable import Auditable


class Schema(Auditable):
    """
    An interface representing a schema in the Catalog. A Schema is a
    basic container of relational objects, like tables, views, etc. A Schema can be self-nested,
    which means it can be schema1.schema2.table.

    This defines the basic properties of a schema. A catalog implementation with SupportsSchemas
    should implement this interface.
    """

    @abstractmethod
    def name(self) -> str:
        """Returns the name of the Schema."""
        pass

    def comment(self) -> Optional[str]:
        """Returns the comment of the Schema. None is returned if the comment is not set."""
        return None

    def properties(self) -> Dict[str, str]:
        """Returns the properties of the Schema. An empty dictionary is returned if no properties are set."""
        return {}
