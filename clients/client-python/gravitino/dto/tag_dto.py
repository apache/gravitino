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
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from dataclasses_json import config, dataclass_json

from gravitino.api.tag.tag import Tag
from gravitino.dto.audit_dto import AuditDTO


@dataclass_json
@dataclass
class TagDTO(Tag):
    """Represents a Tag Data Transfer Object (DTO)."""

    _name: str = field(metadata=config(field_name="name"))
    _comment: str = field(metadata=config(field_name="comment"))
    _properties: dict[str, str] = field(metadata=config(field_name="properties"))

    _audit: AuditDTO = field(default=None, metadata=config(field_name="audit"))
    _inherited: Optional[bool] = field(
        default=None, metadata=config(field_name="inherited")
    )

    def __eq__(self, other: object):
        if not isinstance(other, TagDTO):
            return False
        return (
            self._name == other._name
            and self._comment == other._comment
            and self._properties == other._properties
            and self._audit == other._audit
        )

    def __hash__(self) -> int:
        return hash(
            (
                self._name,
                self._comment,
                frozenset(self._properties.items()) if self._properties else None,
                self._audit,
            )
        )

    @staticmethod
    def builder() -> TagDTO.Builder:
        return TagDTO.Builder()

    def name(self) -> str:
        """Get the name of the tag.

        Returns:
            str: The name of the tag.
        """
        return self._name

    def comment(self) -> str:
        """Get the comment of the tag.

        Returns:
            str: The comment of the tag.
        """
        return self._comment

    def properties(self) -> dict[str, str]:
        """
        Get the properties of the tag.

        Returns:
            dict[str, str]: The properties of the tag.
        """
        return self._properties

    def audit_info(self) -> AuditDTO:
        """
        Get the audit information of the tag.

        Returns:
            AuditDTO: The audit information of the tag.
        """
        return self._audit

    def inherited(self) -> Optional[bool]:
        """Check if the tag is inherited from a parent object or not.

        If the tag is inherited, it will return `True`, if it is owned by the object itself, it will return `False`.

        **Note**. The return value is optional, only when the tag is associated with an object, and called from the
        object, the return value will be present. Otherwise, it will be empty.

        Returns:
            Optional[bool]:
                True if the tag is inherited, false if it is owned by the object itself. Empty if the
                tag is not associated with any object.
        """
        return self._inherited

    class Builder:
        """Helper class to build a TagDTO object."""

        def __init__(self) -> None:
            self._name = ""
            self._comment = ""
            self._properties: dict[str, str] = {}
            self._audit = None
            self._inherited = True

        def name(self, name: str) -> TagDTO.Builder:
            self._name = name
            return self

        def comment(self, comment: str) -> TagDTO.Builder:
            self._comment = comment
            return self

        def properties(self, properties: dict[str, str]) -> TagDTO.Builder:
            self._properties = properties
            return self

        def audit_info(self, audit: AuditDTO) -> TagDTO.Builder:
            self._audit = audit
            return self

        def inherited(self, inherited: bool) -> TagDTO.Builder:
            self._inherited = inherited
            return self

        def build(self) -> TagDTO:
            return TagDTO(
                self._name,
                self._comment,
                self._properties,
                self._audit,
                self._inherited,
            )
