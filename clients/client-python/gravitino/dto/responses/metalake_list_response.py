"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""
from dataclasses import dataclass
from typing import List

from gravitino.dto.metalake_dto import MetalakeDTO
from gravitino.dto.responses.base_response import BaseResponse


@dataclass
class MetalakeListResponse(BaseResponse):
    """Represents a response containing a list of metalakes."""

    metalakes: List[MetalakeDTO]

    def validate(self):
        super().validate()

        if self.metalakes is None:
            raise ValueError("metalakes must be non-null")

        for metalake in self.metalakes:
            if not metalake.name:
                raise ValueError("metalake 'name' must not be null and empty")
            if not metalake.audit:
                raise ValueError("metalake 'audit' must not be null")
