"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

from dataclasses import dataclass, field
from typing import List

from dataclasses_json import config

from gravitino.dto.metalake_dto import MetalakeDTO
from gravitino.dto.responses.base_response import BaseResponse


@dataclass
class MetalakeListResponse(BaseResponse):
    """Represents a response containing a list of metalakes."""

    _metalakes: List[MetalakeDTO] = field(metadata=config(field_name="metalakes"))

    def metalakes(self) -> List[MetalakeDTO]:
        return self._metalakes

    def validate(self):
        """Validates the response data.

        Raises:
            IllegalArgumentException if catalog identifiers are not set.
        """
        super().validate()

        if self._metalakes is None:
            raise ValueError("metalakes must be non-null")

        for metalake in self._metalakes:
            if not metalake.name():
                raise ValueError("metalake 'name' must not be null and empty")
            if not metalake.audit_info():
                raise ValueError("metalake 'audit' must not be null")
