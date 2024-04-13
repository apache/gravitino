"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""
from dataclasses import dataclass
from typing import Optional, Dict

from gravitino.rest.rest_message import RESTRequest


@dataclass
class SchemaCreateRequest(RESTRequest):
    """Represents a request to create a schema."""

    name: str
    comment: Optional[str]
    properties: Optional[Dict[str, str]]

    def validate(self):
        assert self.name is not None, "\"name\" field is required and cannot be empty"
