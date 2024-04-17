"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""
from dataclasses import dataclass
from typing import Optional, Dict

from dataclasses_json import DataClassJsonMixin

from gravitino.api.catalog import Catalog


@dataclass
class CatalogCreateRequest(DataClassJsonMixin):
    """Represents a request to create a catalog."""
    name: str
    type: Catalog.Type
    provider: str
    comment: Optional[str]
    properties: Optional[Dict[str, str]]

    def __init__(self, name: str = None, type: Catalog.Type = Catalog.Type.UNSUPPORTED, provider: str = None,
                 comment: str = None, properties: Dict[str, str] = None):
        self.name = name
        self.type = type
        self.provider = provider
        self.comment = comment
        self.properties = properties

    def validate(self):
        """Validates the fields of the request.

        Raises:
            IllegalArgumentException if name or type are not set.
        """
        assert self.name is not None, "\"name\" field is required and cannot be empty"
        assert self.type is not None, "\"type\" field is required and cannot be empty"
        assert self.provider is not None, "\"provider\" field is required and cannot be empty"
