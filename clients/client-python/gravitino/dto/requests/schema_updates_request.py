"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

from dataclasses import dataclass, field
from typing import List

from dataclasses_json import config

from gravitino.dto.requests.schema_update_request import SchemaUpdateRequest
from gravitino.rest.rest_message import RESTRequest


@dataclass
class SchemaUpdatesRequest(RESTRequest):
    """Represents a request to update a schema."""

    _updates: List[SchemaUpdateRequest] = field(
        metadata=config(field_name="updates"), default_factory=list
    )

    def __init__(self, updates: List[SchemaUpdateRequest]):
        self._updates = updates

    def validate(self):
        """Validates the request.

        Raises:
            IllegalArgumentException If the request is invalid, this exception is thrown.
        """
        if not self._updates:
            raise ValueError("Updates cannot be empty")
        for update_request in self._updates:
            update_request.validate()
