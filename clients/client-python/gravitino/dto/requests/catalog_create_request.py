"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

from dataclasses import dataclass, field
from typing import Optional, Dict

from dataclasses_json import config

from gravitino.api.catalog import Catalog
from gravitino.rest.rest_message import RESTRequest


@dataclass
class CatalogCreateRequest(RESTRequest):
    """Represents a request to create a catalog."""

    _name: str = field(metadata=config(field_name="name"))
    _type: Catalog.Type = field(metadata=config(field_name="type"))
    _provider: str = field(metadata=config(field_name="provider"))
    _comment: Optional[str] = field(metadata=config(field_name="comment"))
    _properties: Optional[Dict[str, str]] = field(
        metadata=config(field_name="properties")
    )

    def __init__(
        self,
        name: str = None,
        type: Catalog.Type = Catalog.Type.UNSUPPORTED,
        provider: str = None,
        comment: str = None,
        properties: Dict[str, str] = None,
    ):
        self._name = name
        self._type = type
        self._provider = provider
        self._comment = comment
        self._properties = properties

    def validate(self):
        """Validates the fields of the request.

        Raises:
            IllegalArgumentException if name or type are not set.
        """
        assert self._name is not None, '"name" field is required and cannot be empty'
        assert self._type is not None, '"type" field is required and cannot be empty'
        assert (
            self._provider is not None
        ), '"provider" field is required and cannot be empty'
