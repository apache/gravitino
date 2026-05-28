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

from typing import Dict

from gravitino.api.audit import Audit
from gravitino.api.metadata_object import MetadataObject
from gravitino.api.metadata_objects import MetadataObjects
from gravitino.api.schema import Schema
from gravitino.api.tag.supports_tags import SupportsTags
from gravitino.api.tag.tag import Tag
from gravitino.client.metadata_object_tag_operations import MetadataObjectTagOperations
from gravitino.dto.schema_dto import SchemaDTO
from gravitino.utils.http_client import HTTPClient


class GenericSchema(
    Schema,
    SupportsTags,
):
    def __init__(
        self,
        schema_dto: SchemaDTO,
        rest_client: HTTPClient,
        metalake: str,
        catalog: str,
    ) -> None:
        self._schema_dto = schema_dto
        metadata_object: MetadataObject = MetadataObjects.of(
            [
                catalog,
                schema_dto.name(),
            ],
            MetadataObject.Type.SCHEMA,
        )
        self._metadata_object_tag_operations = MetadataObjectTagOperations(
            metalake,
            metadata_object,
            rest_client,
        )

    def __eq__(self, value: object) -> bool:
        if not isinstance(value, GenericSchema):
            return False

        return self._schema_dto == value._schema_dto

    def __hash__(self) -> int:
        return hash(self._schema_dto)

    def name(self) -> str:
        return self._schema_dto.name()

    def comment(self) -> str | None:
        return self._schema_dto.comment()

    def properties(self) -> Dict[str, str]:
        return self._schema_dto.properties()

    def audit_info(self) -> Audit:
        return self._schema_dto.audit_info()

    def list_tags(self) -> list[str]:
        return self._metadata_object_tag_operations.list_tags()

    def list_tags_info(self) -> list[Tag]:
        return self._metadata_object_tag_operations.list_tags_info()

    def get_tag(self, name: str) -> Tag:
        return self._metadata_object_tag_operations.get_tag(name)

    def associate_tags(
        self, tags_to_add: list[str], tags_to_remove: list[str]
    ) -> list[str]:
        return self._metadata_object_tag_operations.associate_tags(
            tags_to_add, tags_to_remove
        )

    def supports_tags(self) -> SupportsTags:
        return self
