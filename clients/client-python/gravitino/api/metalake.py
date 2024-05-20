"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

from abc import abstractmethod
from typing import Optional, Dict

from gravitino.api.auditable import Auditable


class Metalake(Auditable):
    """
    The interface of a metalake. The metalake is the top level entity in the gravitino system,
    containing a set of catalogs.
    """

    @abstractmethod
    def name(self) -> str:
        """The name of the metalake.

        Returns:
            str: The name of the metalake.
        """
        pass

    @abstractmethod
    def comment(self) -> Optional[str]:
        """The comment of the metalake. Note. this method will return None if the comment is not set for
        this metalake.

        Returns:
            Optional[str]: The comment of the metalake.
        """
        pass

    @abstractmethod
    def properties(self) -> Optional[Dict[str, str]]:
        """The properties of the metalake. Note, this method will return None if the properties are not
        set.

        Returns:
            Optional[Dict[str, str]]: The properties of the metalake.
        """
        pass
