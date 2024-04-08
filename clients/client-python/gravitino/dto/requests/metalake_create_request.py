"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""
from typing import Optional, Dict

from dataclasses import dataclass

from dataclasses_json import DataClassJsonMixin


@dataclass
class MetalakeCreateRequest(DataClassJsonMixin):
    """"Represents a request to create a Metalake."""
    name: str
    comment: Optional[str]
    properties: Optional[Dict[str, str]]

    def __init__(self, name: str = None, comment: str = None, properties: Dict[str, str] = None):
        super().__init__()

        self.name = name.strip() if name else None
        self.comment = comment.strip() if comment else None
        self.properties = properties

    def validate(self):
        if not self.name:
            raise ValueError("\"name\" field is required and cannot be empty")
