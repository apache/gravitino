"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""
from dataclasses import dataclass, field
from typing import Optional, List

from dataclasses_json import config

from gravitino.dto.responses.base_response import BaseResponse
from gravitino.name_identifier import NameIdentifier


@dataclass
class EntityListResponse(BaseResponse):
    """Represents a response containing a list of catalogs."""
    idents: Optional[List[NameIdentifier]] = field(metadata=config(field_name='identifiers'))

    def validate(self):
        """Validates the response data.

        Raises:
            IllegalArgumentException if catalog identifiers are not set.
        """
        super().validate()

        assert self.idents is not None, "identifiers must not be null"