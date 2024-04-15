"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""
from dataclasses import dataclass, field
from typing import Optional, List

from dataclasses_json import DataClassJsonMixin

from gravitino.dto.requests.schema_update_request import SchemaUpdateRequest


@dataclass
class SchemaUpdatesRequest(DataClassJsonMixin):
    """Represents a request to update a schema."""
    updates: Optional[List[SchemaUpdateRequest]] = field(default_factory=list)

    def validate(self):
        """Validates the request.

        Raises:
            IllegalArgumentException If the request is invalid, this exception is thrown.
        """
        if not self.updates:
            raise ValueError("Updates cannot be empty")
        for update_request in self.updates:
            update_request.validate()