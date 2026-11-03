# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from dataclasses import dataclass, field
from typing import Optional

from dataclasses_json import config, dataclass_json

from gravitino.api.domain.domain import Domain
from gravitino.api.domain.domain_member import DomainMember
from gravitino.api.domain.domain_relation import DomainRelation
from gravitino.api.tag.tag_value import TagValue
from gravitino.dto.audit_dto import AuditDTO
from gravitino.dto.metadata_object_dto import MetadataObjectDTO
from gravitino.utils.precondition import Precondition


@dataclass_json
@dataclass
class DomainBackingTagDTO:
    """The backing tag on the wire."""

    _name: str = field(metadata=config(field_name="name"))
    _value: Optional[str] = field(default=None, metadata=config(field_name="value"))

    def tag_value(self) -> TagValue:
        return TagValue(self._name, self._value)


@dataclass_json
@dataclass
class DomainRelationDTO:
    """A membership relation and optional metadata source."""

    _relation: DomainRelation.Type = field(metadata=config(field_name="relation"))
    _source: Optional[MetadataObjectDTO] = field(
        default=None, metadata=config(field_name="source")
    )

    def relation(self) -> DomainRelation:
        return DomainRelation(self._relation, self._source)


@dataclass_json
@dataclass
class DomainMemberDTO:
    """A member and its relations on the wire."""

    _object: MetadataObjectDTO = field(metadata=config(field_name="object"))
    _relations: list[DomainRelationDTO] = field(metadata=config(field_name="relations"))

    def member(self) -> DomainMember:
        return DomainMember(self._object, tuple(r.relation() for r in self._relations))


@dataclass_json
@dataclass
class DomainDTO:  # pylint: disable=too-many-instance-attributes
    """Domain snapshot data returned by the server."""

    _name: str = field(metadata=config(field_name="name"))
    _properties: dict[str, str] = field(metadata=config(field_name="properties"))
    _status: Domain.Status = field(metadata=config(field_name="status"))
    _audit: AuditDTO = field(metadata=config(field_name="audit"))
    _comment: Optional[str] = field(default=None, metadata=config(field_name="comment"))
    _parent: Optional[str] = field(default=None, metadata=config(field_name="parent"))
    _backing_tag: Optional[DomainBackingTagDTO] = field(
        default=None, metadata=config(field_name="backingTag")
    )
    _relations: list[DomainRelationDTO] = field(
        default_factory=list, metadata=config(field_name="relations")
    )

    def name(self) -> str:
        return self._name

    def comment(self) -> Optional[str]:
        return self._comment

    def properties(self) -> dict[str, str]:
        return dict(self._properties)

    def status(self) -> Domain.Status:
        return self._status

    def parent(self) -> Optional[str]:
        return self._parent

    def backing_tag(self) -> Optional[TagValue]:
        return self._backing_tag.tag_value() if self._backing_tag else None

    def audit_info(self) -> AuditDTO:
        return self._audit

    def relations(self) -> list[DomainRelation]:
        return [r.relation() for r in self._relations]

    def validate(self) -> None:
        Precondition.check_argument(
            isinstance(self._name, str) and bool(self._name.strip()),
            "Domain response must have a name",
        )
        Precondition.check_argument(
            isinstance(self._status, Domain.Status),
            "Domain response must have a status",
        )
        Precondition.check_argument(
            self._properties is not None, "Domain response must have properties"
        )
        Precondition.check_argument(
            self._audit is not None, "Domain response must have audit information"
        )
