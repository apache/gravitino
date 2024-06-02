from dataclasses import dataclass, field
from dataclasses_json import config

from .base_response import BaseResponse
from ..version_dto import VersionDTO


@dataclass
class VersionResponse(BaseResponse):
    """Represents a response containing version of Gravitino."""

    _version: VersionDTO = field(metadata=config(field_name="version"))

    def version(self) -> VersionDTO:
        return self._version

    def validate(self):
        """Validates the response data.

        Raise:
            IllegalArgumentException if name or audit information is not set.
        """
        super().validate()

        assert self._version is not None, "version must be non-null"
        assert (
            self._version.version() is not None
        ), "version 'version' must not be null and empty"
        assert (
            self._version.compile_date() is not None
        ), "version 'compile_date' must not be null and empty"
        assert (
            self._version.git_commit() is not None
        ), "version 'git_commit' must not be null and empty"
