"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

from dataclasses import dataclass, field

from dataclasses_json import config

from gravitino.dto.fileset_dto import FilesetDTO
from gravitino.dto.responses.base_response import BaseResponse


@dataclass
class FilesetResponse(BaseResponse):
    """Response for fileset creation."""

    _fileset: FilesetDTO = field(metadata=config(field_name="fileset"))

    def fileset(self) -> FilesetDTO:
        return self._fileset

    def validate(self):
        """Validates the response data.

        Raises:
            IllegalArgumentException if catalog identifiers are not set.
        """
        super().validate()
        assert self._fileset is not None, "fileset must not be null"
        assert self._fileset.name, "fileset 'name' must not be null and empty"
        assert (
            self._fileset.storage_location
        ), "fileset 'storageLocation' must not be null and empty"
        assert (
            self._fileset.type is not None
        ), "fileset 'type' must not be null and empty"
