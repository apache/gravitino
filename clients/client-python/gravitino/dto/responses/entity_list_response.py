"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

from dataclasses import dataclass, field
from typing import List

from dataclasses_json import config

from gravitino.dto.responses.base_response import BaseResponse
from gravitino.name_identifier import NameIdentifier


@dataclass
class EntityListResponse(BaseResponse):
    """Represents a response containing a list of catalogs."""

    _idents: List[NameIdentifier] = field(metadata=config(field_name="identifiers"))

    def identifiers(self) -> List[NameIdentifier]:
        return self._idents

    def validate(self):
        """Validates the response data.

        Raises:
            IllegalArgumentException if catalog identifiers are not set.
        """
        super().validate()

        assert self._idents is not None, "identifiers must not be null"
