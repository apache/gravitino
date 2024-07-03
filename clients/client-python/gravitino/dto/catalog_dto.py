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

from typing import Dict
from dataclasses import dataclass, field

from dataclasses_json import config

from .audit_dto import AuditDTO
from ..api.catalog import Catalog


@dataclass
class CatalogDTO(Catalog):
    """Data transfer object representing catalog information."""

    _name: str = field(metadata=config(field_name="name"))
    _type: Catalog.Type = field(metadata=config(field_name="type"))
    _provider: str = field(metadata=config(field_name="provider"))
    _comment: str = field(metadata=config(field_name="comment"))
    _properties: Dict[str, str] = field(metadata=config(field_name="properties"))
    _audit: AuditDTO = field(default=None, metadata=config(field_name="audit"))

    def builder(
        self,
        name: str = None,
        catalog_type: Catalog.Type = Catalog.Type.UNSUPPORTED,
        provider: str = None,
        comment: str = None,
        properties: Dict[str, str] = None,
        audit: AuditDTO = None,
    ):
        self._name = name
        self._type = catalog_type
        self._provider = provider
        self._comment = comment
        self._properties = properties
        self._audit = audit

    def name(self) -> str:
        return self._name

    def type(self) -> Catalog.Type:
        return self._type

    def provider(self) -> str:
        return self._provider

    def comment(self) -> str:
        return self._comment

    def properties(self) -> Dict[str, str]:
        return self._properties

    def audit_info(self) -> AuditDTO:
        return self._audit
