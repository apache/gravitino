"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""
from dataclasses import dataclass
from typing import List

from dataclasses_json import DataClassJsonMixin

from gravitino.dto.requests.metalake_update_request import MetalakeUpdateRequest


@dataclass
class MetalakeUpdatesRequest(DataClassJsonMixin):
    """Represents a request containing multiple Metalake updates."""

    updates: List[MetalakeUpdateRequest]

    def __init__(self, updates: List[MetalakeUpdateRequest]):
        """Constructor for MetalakeUpdatesRequest.

        Args:
            updates: The list of Metalake update requests.
        """
        self.updates = updates

    def validate(self):
        """Validates each request in the list.

        Raises:
            IllegalArgumentException if validation of any request fails.
        """
        for update in self.updates:
            update.validate()
