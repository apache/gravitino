"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

from dataclasses import dataclass, field
from typing import Optional

from dataclasses_json import config

from gravitino.dto.metalake_dto import MetalakeDTO
from gravitino.dto.responses.base_response import BaseResponse


@dataclass
class MetalakeResponse(BaseResponse):
    """Represents a response containing metalake information."""

    _metalake: Optional[MetalakeDTO] = field(metadata=config(field_name="metalake"))

    def metalake(self) -> MetalakeDTO:
        return self._metalake

    def validate(self):
        """Validates the response data.
        TODO: @throws IllegalArgumentException if the metalake, name or audit information is not set.
        """
        super().validate()

        assert self._metalake is not None, "metalake must not be null"
        assert (
            self._metalake.name() is not None
        ), "metalake 'name' must not be null and empty"
        assert (
            self._metalake.audit_info() is not None
        ), "metalake 'audit' must not be null"
