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

from dataclasses_json import DataClassJsonMixin, config

from gravitino.api.audit import Audit
from gravitino.dto.audit_dto import AuditDTO
from gravitino.api.metalake import Metalake


@dataclass
class MetalakeDTO(Metalake, DataClassJsonMixin):
    """Represents a Metalake Data Transfer Object (DTO) that implements the Metalake interface."""

    _name: str = field(metadata=config(field_name="name"))
    """The name of the Metalake DTO."""

    _comment: Optional[str] = field(metadata=config(field_name="comment"))
    """The comment of the Metalake DTO."""

    _properties: Optional[Dict[str, str]] = field(
        metadata=config(field_name="properties")
    )
    """The properties of the Metalake DTO."""

    _audit: Optional[AuditDTO] = field(metadata=config(field_name="audit"))
    """The audit information of the Metalake DTO."""

    def name(self) -> str:
        return self._name

    def comment(self) -> str:
        return self._comment

    def properties(self) -> Dict[str, str]:
        return self._properties

    def audit_info(self) -> Audit:
        return self._audit

    def equals(self, other):
        if self == other:
            return True
        if not isinstance(other, MetalakeDTO):
            return False
        return (
            self.name() == other.name()
            and self.comment() == other.comment()
            and self.property_equal(self.properties(), other.properties())
            and self.audit_info() == other.audit_info()
        )

    def property_equal(self, p1, p2):
        if p1 is None and p2 is None:
            return True
        if p1 is not None and not p1 and p2 is None:
            return True
        if p2 is not None and not p2 and p1 is None:
            return True
        return p1 == p2
