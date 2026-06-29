# Licensed to the Apache Software Foundation (ASF) under one
# (license header same as before)
"""View interface for Gravitino Python client."""

from abc import ABC, abstractmethod
from typing import Optional

from gravitino.api.auditable import Auditable


class View(Auditable, ABC):
    """The `View` interface defines the metadata of a view."""

    @abstractmethod
    def name(self) -> str:
        """Get the name of the view."""
        pass

    @abstractmethod
    def comment(self) -> Optional[str]:
        """Get the comment of the view."""
        return None

    @abstractmethod
    def columns(self) -> list:
        """Get the output columns of the view."""
        return []

    @abstractmethod
    def representations(self) -> list[dict]:
        """Get the view representations."""
        pass

    @abstractmethod
    def default_catalog(self) -> Optional[str]:
        """Get the default catalog."""
        return None

    @abstractmethod
    def default_schema(self) -> Optional[str]:
        """Get the default schema."""
        return None

    @abstractmethod
    def properties(self) -> dict[str, str]:
        """Get the properties of the view."""
        return {}