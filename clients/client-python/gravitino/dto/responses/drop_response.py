"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""
from gravitino.dto.responses.base_response import BaseResponse


class DropResponse(BaseResponse):
    """Represents a response for a drop operation."""

    dropped : bool

    def dropped(self) -> bool:
        return self.dropped
