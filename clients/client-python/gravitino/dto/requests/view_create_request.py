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

from dataclasses_json import config

from gravitino.dto.rel.column_dto import ColumnDTO
from gravitino.dto.rel.json_serdes.representation_serdes import RepresentationSerdes
from gravitino.dto.rel.representation_dto import RepresentationDTO
from gravitino.dto.rel.sql_representation_dto import SQLRepresentationDTO
from gravitino.rest.rest_message import RESTRequest
from gravitino.utils.precondition import Precondition


@dataclass
class ViewCreateRequest(RESTRequest):
    """Represents a request to create a view."""

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
            exclude=lambda value: value is None,
        ),
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

    def validate(self) -> None:
        Precondition.check_string_not_empty(
            self._name, '"name" field is required and cannot be empty'
        )
        Precondition.check_argument(
            self._representations is not None and len(self._representations) > 0,
            '"representations" field is required and cannot be empty',
        )
        for representation in self._representations:
            Precondition.check_argument(
                representation is not None, "representation must not be null"
            )
            representation.validate()
        if self._columns:
            for column in self._columns:
                Precondition.check_argument(
                    column is not None, "column must not be null"
                )
                column.validate()
        ViewCreateRequest.validate_no_duplicate_dialects(self._representations)

    @staticmethod
    def validate_no_duplicate_dialects(
        representations: list[RepresentationDTO],
    ) -> None:
        """Validate that SQL representations do not use duplicate dialects."""
        seen_dialects = set()
        for representation in representations:
            if isinstance(representation, SQLRepresentationDTO):
                Precondition.check_argument(
                    representation.dialect() not in seen_dialects,
                    f"Duplicate SQL representation dialect: {representation.dialect()}",
                )
                seen_dialects.add(representation.dialect())
