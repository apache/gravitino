"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

from dataclasses import dataclass, field
from typing import Optional, Dict

from dataclasses_json import config

from gravitino.rest.rest_message import RESTRequest


@dataclass
class SchemaCreateRequest(RESTRequest):
    """Represents a request to create a schema."""

    _name: str = field(metadata=config(field_name="name"))
    _comment: Optional[str] = field(metadata=config(field_name="comment"))
    _properties: Optional[Dict[str, str]] = field(
        metadata=config(field_name="properties")
    )

    def __init__(
        self, name: str, comment: Optional[str], properties: Optional[Dict[str, str]]
    ):
        self._name = name
        self._comment = comment
        self._properties = properties

    def validate(self):
        assert self._name is not None, '"name" field is required and cannot be empty'
