"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

from dataclasses import dataclass, field
from typing import List

from dataclasses_json import config

from gravitino.dto.requests.fileset_update_request import FilesetUpdateRequest
from gravitino.rest.rest_message import RESTRequest


@dataclass
class FilesetUpdatesRequest(RESTRequest):
    """Request to represent updates to a fileset."""

    _updates: List[FilesetUpdateRequest] = field(
        metadata=config(field_name="updates"), default_factory=list
    )

    def validate(self):
        if not self._updates:
            raise ValueError("Updates cannot be empty")
        for update_request in self._updates:
            update_request.validate()
