"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
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

        assert self._catalog is not None, "catalog must not be null"
        assert (
            self._catalog.name() is not None
        ), "catalog 'name' must not be null and empty"
        assert self._catalog.type() is not None, "catalog 'type' must not be null"
        assert (
            self._catalog.audit_info() is not None
        ), "catalog 'audit' must not be null"

    def catalog(self) -> CatalogDTO:
        return self._catalog
