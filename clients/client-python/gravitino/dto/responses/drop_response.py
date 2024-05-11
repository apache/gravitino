"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

from dataclasses import dataclass, field

from dataclasses_json import config

from gravitino.dto.responses.base_response import BaseResponse


@dataclass
class DropResponse(BaseResponse):
    """Represents a response for a drop operation."""

    _dropped: bool = field(metadata=config(field_name="dropped"))

    def dropped(self) -> bool:
        return self._dropped
