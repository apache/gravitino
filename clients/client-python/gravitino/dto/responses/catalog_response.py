"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

from dataclasses import dataclass, field

from dataclasses_json import config

from .base_response import BaseResponse
from ..catalog_dto import CatalogDTO


@dataclass
class CatalogResponse(BaseResponse):
    """Represents a response containing catalog information."""

    _catalog: CatalogDTO = field(metadata=config(field_name="catalog"))

    def validate(self):
        """Validates the response data.

        Raises:
            IllegalArgumentException if the catalog name, type or audit is not set.
        """
        super().validate()

        assert self.catalog is not None, "catalog must not be null"
        assert (
            self.catalog.name() is not None
        ), "catalog 'name' must not be null and empty"
        assert self.catalog.type() is not None, "catalog 'type' must not be null"
        assert self.catalog.audit_info() is not None, "catalog 'audit' must not be null"

    def catalog(self) -> CatalogDTO:
        return self._catalog
