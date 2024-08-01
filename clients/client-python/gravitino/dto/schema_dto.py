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
from typing import Optional, Dict

from dataclasses_json import config

from gravitino.api.audit import Audit
from gravitino.api.schema import Schema
from gravitino.dto.audit_dto import AuditDTO


@dataclass
class SchemaDTO(Schema):
    """Represents a Schema DTO (Data Transfer Object)."""

    _name: str = field(metadata=config(field_name="name"))
    """The name of the Metalake DTO."""

    _comment: Optional[str] = field(metadata=config(field_name="comment"))
    """The comment of the Metalake DTO."""

    _properties: Optional[Dict[str, str]] = field(
        metadata=config(field_name="properties")
    )
    """The properties of the Metalake DTO."""

    _audit: AuditDTO = field(metadata=config(field_name="audit"))
    """The audit information of the Metalake DTO."""

    def name(self) -> str:
        return self._name

    def audit_info(self) -> Audit:
        return self._audit

    def comment(self) -> Optional[str]:
        """Returns the comment of the Schema. None is returned if the comment is not set."""
        return self._comment

    def properties(self) -> Dict[str, str]:
        """Returns the properties of the Schema. An empty dictionary is returned if no properties are set."""
        return self._properties
