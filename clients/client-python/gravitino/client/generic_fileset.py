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
from typing import Dict, List, Optional

from gravitino.api.credential.credential import Credential
from gravitino.api.credential.supports_credentials import SupportsCredentials
from gravitino.api.file.fileset import Fileset
from gravitino.api.metadata_object import MetadataObject
from gravitino.api.metadata_objects import MetadataObjects
from gravitino.api.tag.supports_tags import SupportsTags
from gravitino.api.tag.tag import Tag
from gravitino.client.metadata_object_credential_operations import (
    MetadataObjectCredentialOperations,
)
from gravitino.client.metadata_object_tag_operations import MetadataObjectTagOperations
from gravitino.dto.audit_dto import AuditDTO
from gravitino.dto.fileset_dto import FilesetDTO
from gravitino.namespace import Namespace
from gravitino.utils import HTTPClient


class GenericFileset(
    Fileset,
    SupportsCredentials,
    SupportsTags,
):
    _fileset: FilesetDTO
    """The fileset data transfer object"""

    _object_credential_operations: MetadataObjectCredentialOperations
    """The metadata object credential operations"""

    def __init__(
        self, fileset: FilesetDTO, rest_client: HTTPClient, full_namespace: Namespace
    ):
        self._fileset = fileset
        metadata_object = MetadataObjects.of(
            [full_namespace.level(1), full_namespace.level(2), fileset.name()],
            MetadataObject.Type.FILESET,
        )
        self._object_credential_operations = MetadataObjectCredentialOperations(
            full_namespace.level(0), metadata_object, rest_client
        )
        self._object_tag_operations = MetadataObjectTagOperations(
            full_namespace.level(0), metadata_object, rest_client
        )

    def name(self) -> str:
        return self._fileset.name()

    def type(self) -> Fileset.Type:
        return self._fileset.type()

    def storage_locations(self) -> Dict[str, str]:
        return self._fileset.storage_locations()

    def comment(self) -> Optional[str]:
        return self._fileset.comment()

    def properties(self) -> Dict[str, str]:
        return self._fileset.properties()

    def audit_info(self) -> AuditDTO:
        return self._fileset.audit_info()

    def support_credentials(self) -> SupportsCredentials:
        return self

    def supports_tags(self) -> SupportsTags:
        return self

    def get_credentials(self) -> List[Credential]:
        return self._object_credential_operations.get_credentials()

    def list_tags(self) -> List[str]:
        return self._object_tag_operations.list_tags()

    def list_tags_info(self) -> List[Tag]:
        return self._object_tag_operations.list_tags_info()

    def get_tag(self, name: str) -> Tag:
        return self._object_tag_operations.get_tag(name)

    def associate_tags(
        self, tags_to_add: List[str], tags_to_remove: List[str]
    ) -> List[str]:
        return self._object_tag_operations.associate_tags(tags_to_add, tags_to_remove)
