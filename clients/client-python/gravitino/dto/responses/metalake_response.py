"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""
from dataclasses import dataclass
from typing import Optional

from gravitino.dto.metalake_dto import MetalakeDTO
from gravitino.dto.responses.base_response import BaseResponse


@dataclass
class MetalakeResponse(BaseResponse):
    """Represents a response containing metalake information."""

    metalake: Optional[MetalakeDTO]

    def validate(self):
        """Validates the response data.
        TODO: @throws IllegalArgumentException if the metalake, name or audit information is not set.
        """
        super().validate()

        assert self.metalake is not None, "metalake must not be null"
        assert self.metalake.name is not None, "metalake 'name' must not be null and empty"
        assert self.metalake.audit is not None, "metalake 'audit' must not be null"
