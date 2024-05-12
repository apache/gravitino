"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

from typing import Optional, Dict

from dataclasses import dataclass, field

from dataclasses_json import config

from gravitino.rest.rest_message import RESTRequest


@dataclass
class MetalakeCreateRequest(RESTRequest):
    """Represents a request to create a Metalake."""

    _name: str = field(metadata=config(field_name="name"))
    _comment: Optional[str] = field(metadata=config(field_name="comment"))
    _properties: Optional[Dict[str, str]] = field(
        metadata=config(field_name="properties")
    )

    def __init__(
        self, name: str = None, comment: str = None, properties: Dict[str, str] = None
    ):
        super().__init__()

        self._name = name.strip() if name else None
        self._comment = comment.strip() if comment else None
        self._properties = properties

    def name(self) -> str:
        return self._name

    def validate(self):
        if not self._name:
            raise ValueError('"name" field is required and cannot be empty')
