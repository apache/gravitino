"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""
from dataclasses import dataclass, field
from typing import Optional, Dict

from dataclasses_json import DataClassJsonMixin, config

from gravitino.api.fileset import Fileset


@dataclass
class FilesetCreateRequest(DataClassJsonMixin):
    """Represents a request to create a fileset."""
    name: str
    comment: Optional[str]
    type: Fileset.Type
    storage_location: str = field(metadata=config(field_name='storageLocation'))
    properties: Dict[str, str]

    def validate(self):
        """Validates the request.

        Raises:
            IllegalArgumentException if the request is invalid.
        """
        if not self.name:
            raise ValueError('"name" field is required and cannot be empty')
