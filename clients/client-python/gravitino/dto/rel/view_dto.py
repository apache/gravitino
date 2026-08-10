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

from dataclasses_json import DataClassJsonMixin, config

from gravitino.api.rel.column import Column
from gravitino.api.rel.representation import Representation
from gravitino.api.rel.view import View
from gravitino.dto.audit_dto import AuditDTO
from gravitino.dto.rel.column_dto import ColumnDTO
from gravitino.dto.rel.json_serdes.representation_serdes import RepresentationSerdes
from gravitino.dto.rel.representation_dto import RepresentationDTO
from gravitino.dto.util.dto_converters import DTOConverters
from gravitino.utils.precondition import Precondition


@dataclass
class ViewDTO(View, DataClassJsonMixin):  # pylint: disable=too-many-instance-attributes
    """Represents a View DTO."""

    _name: str = field(metadata=config(field_name="name"))
    _columns: Optional[list[ColumnDTO]] = field(
        default=None, metadata=config(field_name="columns")
    )
    _representations: Optional[list[RepresentationDTO]] = field(
        default=None,
        metadata=config(
            field_name="representations",
            encoder=lambda items: [
                RepresentationSerdes.serialize(item) for item in items
            ],
            decoder=lambda values: [
                RepresentationSerdes.deserialize(value) for value in values
            ],
        ),
    )
    _audit: Optional[AuditDTO] = field(
        default=None, metadata=config(field_name="audit")
    )
    _comment: Optional[str] = field(default=None, metadata=config(field_name="comment"))
    _default_catalog: Optional[str] = field(
        default=None, metadata=config(field_name="defaultCatalog")
    )
    _default_schema: Optional[str] = field(
        default=None, metadata=config(field_name="defaultSchema")
    )
    _properties: Optional[dict[str, str]] = field(
        default=None, metadata=config(field_name="properties")
    )

    def __post_init__(self):
        if self._columns is None:
            self._columns = []
        Precondition.check_string_not_empty(self._name, "name cannot be null or empty")
        Precondition.check_argument(self._audit is not None, "audit cannot be null")
        Precondition.check_argument(
            self._representations is not None and len(self._representations) > 0,
            "representations cannot be null or empty",
        )
        for representation in self._representations:
            Precondition.check_argument(
                representation is not None, "representation must not be null"
            )
            representation.validate()

    def name(self) -> str:
        return self._name

    def comment(self) -> Optional[str]:
        return self._comment

    def columns(self) -> list[Column]:
        return self._columns

    def representations(self) -> list[Representation]:
        return DTOConverters.from_dtos(self._representations)

    def default_catalog(self) -> Optional[str]:
        return self._default_catalog

    def default_schema(self) -> Optional[str]:
        return self._default_schema

    def properties(self) -> dict[str, str]:
        return self._properties or {}

    def audit_info(self) -> AuditDTO:
        return self._audit
