"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

from dataclasses import dataclass, field

from dataclasses_json import DataClassJsonMixin, config

from gravitino.dto.responses.base_response import BaseResponse
from gravitino.dto.schema_dto import SchemaDTO


@dataclass
class SchemaResponse(BaseResponse, DataClassJsonMixin):
    """Represents a response for a schema."""

    _schema: SchemaDTO = field(metadata=config(field_name="schema"))

    # TODO
    # pylint: disable=arguments-differ
    def schema(self) -> SchemaDTO:
        return self._schema

    def validate(self):
        """Validates the response data.

        Raises:
            IllegalArgumentException if catalog identifiers are not set.
        """
        super().validate()

        assert self._schema is not None, "schema must be non-null"
        assert (
            self._schema.name() is not None
        ), "schema 'name' must not be null and empty"
        assert self._schema.audit_info() is not None, "schema 'audit' must not be null"
