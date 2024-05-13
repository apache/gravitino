"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

from dataclasses import dataclass, field
from typing import List

from dataclasses_json import config

from gravitino.dto.requests.metalake_update_request import MetalakeUpdateRequest
from gravitino.rest.rest_message import RESTRequest


@dataclass
class MetalakeUpdatesRequest(RESTRequest):
    """Represents a request containing multiple Metalake updates."""

    _updates: List[MetalakeUpdateRequest] = field(metadata=config(field_name="updates"))

    def __init__(self, updates: List[MetalakeUpdateRequest]):
        """Constructor for MetalakeUpdatesRequest.

        Args:
            updates: The list of Metalake update requests.
        """
        self._updates = updates

    def validate(self):
        """Validates each request in the list.

        Raises:
            IllegalArgumentException if validation of any request fails.
        """
        for update in self._updates:
            update.validate()
