"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

from dataclasses import dataclass, field
from typing import List

from dataclasses_json import config

from .base_response import BaseResponse
from ..catalog_dto import CatalogDTO


@dataclass
class CatalogListResponse(BaseResponse):
    """Represents a response for a list of catalogs with their information."""

    _catalogs: List[CatalogDTO] = field(metadata=config(field_name="catalogs"))

    def catalogs(self) -> List[CatalogDTO]:
        return self._catalogs
