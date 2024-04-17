"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""
from dataclasses import dataclass
from typing import Optional, List

from dataclasses_json import DataClassJsonMixin

from gravitino.dto.requests.catalog_update_request import CatalogUpdateRequest


@dataclass
class CatalogUpdatesRequest(DataClassJsonMixin):
    """Represents a request containing multiple catalog updates."""
    updates: Optional[List[CatalogUpdateRequest]]

    def __init__(self, updates: List[CatalogUpdateRequest] = None):
        self.updates = updates

    def validate(self):
        """Validates each request in the list.

        Raises:
            IllegalArgumentException if validation of any request fails.
        """
        if self.updates is not None:
            for update_request in self.updates:
                update_request.validate()
        else:
            raise ValueError("Updates cannot be null")
