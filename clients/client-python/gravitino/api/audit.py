"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

from abc import ABC, abstractmethod
from datetime import datetime


class Audit(ABC):
    """Represents the audit information of an entity."""

    @abstractmethod
    def creator(self) -> str:
        """The creator of the entity.

        Returns:
             the creator of the entity.
        """
        pass

    @abstractmethod
    def create_time(self) -> datetime:
        """The creation time of the entity.

        Returns:
             The creation time of the entity.
        """
        pass

    @abstractmethod
    def last_modifier(self) -> str:
        """
        Returns:
             The last modifier of the entity.
        """
        pass

    @abstractmethod
    def last_modified_time(self) -> datetime:
        """
        Returns:
             The last modified time of the entity.
        """
        pass
