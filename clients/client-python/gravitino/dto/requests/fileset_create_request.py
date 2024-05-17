"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

from dataclasses import dataclass, field
from typing import Optional, Dict

from dataclasses_json import config

from gravitino.api.fileset import Fileset
from gravitino.rest.rest_message import RESTRequest


@dataclass
class FilesetCreateRequest(RESTRequest):
    """Represents a request to create a fileset."""

    _name: str = field(metadata=config(field_name="name"))
    _comment: Optional[str] = field(metadata=config(field_name="comment"))
    _type: Optional[Fileset.Type] = field(metadata=config(field_name="type"))
    _storage_location: Optional[str] = field(
        metadata=config(field_name="storageLocation")
    )
    _properties: Optional[Dict[str, str]] = field(
        metadata=config(field_name="properties")
    )

    def __init__(
        self,
        name: str,
        comment: Optional[str] = None,
        type: Fileset.Type = None,
        storage_location: Optional[str] = None,
        properties: Optional[Dict[str, str]] = None,
    ):
        self._name = name
        self._comment = comment
        self._type = type
        self._storage_location = storage_location
        self._properties = properties

    def validate(self):
        """Validates the request.

        Raises:
            IllegalArgumentException if the request is invalid.
        """
        if not self._name:
            raise ValueError('"name" field is required and cannot be empty')
