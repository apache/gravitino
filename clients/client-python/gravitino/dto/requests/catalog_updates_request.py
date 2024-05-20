"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

from dataclasses import dataclass, field
from typing import Optional, List

from dataclasses_json import config

from gravitino.dto.requests.catalog_update_request import CatalogUpdateRequest
from gravitino.rest.rest_message import RESTRequest


@dataclass
class CatalogUpdatesRequest(RESTRequest):
    """Represents a request containing multiple catalog updates."""

    _updates: Optional[List[CatalogUpdateRequest]] = field(
        metadata=config(field_name="updates"), default_factory=list
    )

    def __init__(self, updates: List[CatalogUpdateRequest] = None):
        self._updates = updates

    def validate(self):
        """Validates each request in the list.

        Raises:
            IllegalArgumentException if validation of any request fails.
        """
        if self._updates is not None:
            for update_request in self._updates:
                update_request.validate()
        else:
            raise ValueError("Updates cannot be null")
