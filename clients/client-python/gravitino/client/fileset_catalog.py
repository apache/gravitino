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

import logging
from typing import List, Dict

from gravitino.api.catalog import Catalog
from gravitino.api.credential.supports_credentials import SupportsCredentials
from gravitino.api.credential.credential import Credential
from gravitino.api.file.fileset import Fileset
from gravitino.api.file.fileset_change import FilesetChange
from gravitino.audit.caller_context import CallerContextHolder, CallerContext
from gravitino.client.base_schema_catalog import BaseSchemaCatalog
from gravitino.client.generic_fileset import GenericFileset
from gravitino.dto.audit_dto import AuditDTO
from gravitino.dto.requests.fileset_create_request import FilesetCreateRequest
from gravitino.dto.requests.fileset_update_request import FilesetUpdateRequest
from gravitino.dto.requests.fileset_updates_request import FilesetUpdatesRequest
from gravitino.dto.responses.drop_response import DropResponse
from gravitino.dto.responses.entity_list_response import EntityListResponse
from gravitino.dto.responses.file_location_response import FileLocationResponse
from gravitino.dto.responses.fileset_response import FilesetResponse
from gravitino.name_identifier import NameIdentifier
from gravitino.namespace import Namespace
from gravitino.utils import HTTPClient
from gravitino.rest.rest_utils import encode_string
from gravitino.exceptions.handlers.fileset_error_handler import FILESET_ERROR_HANDLER

logger = logging.getLogger(__name__)


class FilesetCatalog(BaseSchemaCatalog, SupportsCredentials):
    """
    Fileset catalog is a catalog implementation that supports fileset like metadata operations, for
    example, schemas and filesets list, creation, update and deletion. A Fileset catalog is under the metalake.
    """

    def __init__(
        self,
        namespace: Namespace,
        name: str = None,
        catalog_type: Catalog.Type = Catalog.Type.UNSUPPORTED,
        provider: str = None,
        comment: str = None,
        properties: Dict[str, str] = None,
        audit: AuditDTO = None,
        rest_client: HTTPClient = None,
    ):

        super().__init__(
            namespace,
            name,
            catalog_type,
            provider,
            comment,
            properties,
            audit,
            rest_client,
        )

    def as_fileset_catalog(self):
        return self

    def list_filesets(self, namespace: Namespace) -> List[NameIdentifier]:
        """List the filesets in a schema namespace from the catalog.

        Args:
            namespace: A schema namespace. This namespace should have 1 level, which is the schema name

        Raises:
            NoSuchSchemaException If the schema does not exist.

        Returns:
            A list of NameIdentifier of filesets under the given namespace.
        """

        self.check_fileset_namespace(namespace)

        full_namespace = self._get_fileset_full_namespace(namespace)

        resp = self.rest_client.get(
            self.format_fileset_request_path(full_namespace),
            error_handler=FILESET_ERROR_HANDLER,
        )
        entity_list_resp = EntityListResponse.from_json(resp.body, infer_missing=True)
        entity_list_resp.validate()

        return [
            NameIdentifier.of(ident.namespace().level(2), ident.name())
            for ident in entity_list_resp.identifiers()
        ]

    def load_fileset(self, ident: NameIdentifier) -> Fileset:
        """Load fileset metadata by {@link NameIdentifier} from the catalog.

        Args:
            ident: A fileset identifier, which should be "schema.fileset" format.

        Raises:
            NoSuchFilesetException If the fileset does not exist.

        Returns:
            The fileset metadata.
        """
        self.check_fileset_name_identifier(ident)

        full_namespace = self._get_fileset_full_namespace(ident.namespace())

        resp = self.rest_client.get(
            f"{self.format_fileset_request_path(full_namespace)}/{encode_string(ident.name())}",
            error_handler=FILESET_ERROR_HANDLER,
        )
        fileset_resp = FilesetResponse.from_json(resp.body, infer_missing=True)
        fileset_resp.validate()

        return GenericFileset(fileset_resp.fileset(), self.rest_client, full_namespace)

    def create_fileset(
        self,
        ident: NameIdentifier,
        comment: str,
        fileset_type: Fileset.Type,
        storage_location: str,
        properties: Dict[str, str],
    ) -> Fileset:
        """Create a fileset metadata in the catalog.

        If the type of the fileset object is "MANAGED", the underlying storageLocation can be null,
        and Gravitino will manage the storage location based on the location of the schema.

        If the type of the fileset object is "EXTERNAL", the underlying storageLocation must be set.

        Args:
            ident: A fileset identifier, which should be "schema.fileset" format.
            comment: The comment of the fileset.
            fileset_type: The type of the fileset.
            storage_location: The storage location of the fileset.
            properties: The properties of the fileset.

        Raises:
            NoSuchSchemaException If the schema does not exist.
            FilesetAlreadyExistsException If the fileset already exists.

        Returns:
            The created fileset metadata
        """
        self.check_fileset_name_identifier(ident)

        full_namespace = self._get_fileset_full_namespace(ident.namespace())

        req = FilesetCreateRequest(
            name=encode_string(ident.name()),
            comment=comment,
            fileset_type=fileset_type,
            storage_location=storage_location,
            properties=properties,
        )

        resp = self.rest_client.post(
            self.format_fileset_request_path(full_namespace),
            req,
            error_handler=FILESET_ERROR_HANDLER,
        )
        fileset_resp = FilesetResponse.from_json(resp.body, infer_missing=True)
        fileset_resp.validate()

        return GenericFileset(fileset_resp.fileset(), self.rest_client, full_namespace)

    def alter_fileset(self, ident: NameIdentifier, *changes) -> Fileset:
        """Update a fileset metadata in the catalog.

        Args:
            ident: A fileset identifier, which should be "schema.fileset" format.
            changes: The changes to apply to the fileset.

        Raises:
            IllegalArgumentException If the changes are invalid.
            NoSuchFilesetException If the fileset does not exist.

        Returns:
            The updated fileset metadata.
        """
        self.check_fileset_name_identifier(ident)

        full_namespace = self._get_fileset_full_namespace(ident.namespace())

        updates = [
            FilesetCatalog.to_fileset_update_request(change) for change in changes
        ]
        req = FilesetUpdatesRequest(updates)
        req.validate()

        resp = self.rest_client.put(
            f"{self.format_fileset_request_path(full_namespace)}/{encode_string(ident.name())}",
            req,
            error_handler=FILESET_ERROR_HANDLER,
        )
        fileset_resp = FilesetResponse.from_json(resp.body, infer_missing=True)
        fileset_resp.validate()

        return GenericFileset(fileset_resp.fileset(), self.rest_client, full_namespace)

    def drop_fileset(self, ident: NameIdentifier) -> bool:
        """Drop a fileset from the catalog.

        The underlying files will be deleted if this fileset type is managed, otherwise, only the
        metadata will be dropped.

        Args:
             ident: A fileset identifier, which should be "schema.fileset" format.

        Returns:
             true If the fileset is dropped, false the fileset did not exist.
        """
        self.check_fileset_name_identifier(ident)

        full_namespace = self._get_fileset_full_namespace(ident.namespace())

        resp = self.rest_client.delete(
            f"{self.format_fileset_request_path(full_namespace)}/{encode_string(ident.name())}",
            error_handler=FILESET_ERROR_HANDLER,
        )
        drop_resp = DropResponse.from_json(resp.body, infer_missing=True)
        drop_resp.validate()

        return drop_resp.dropped()

    def get_file_location(self, ident: NameIdentifier, sub_path: str) -> str:
        """Get the actual location of a file or directory based on the storage location of Fileset and the sub path.

        Args:
             ident: A fileset identifier, which should be "schema.fileset" format.
             sub_path: The sub path of the file or directory.

        Returns:
             The actual location of the file or directory.
        """
        self.check_fileset_name_identifier(ident)

        full_namespace = self._get_fileset_full_namespace(ident.namespace())
        try:
            caller_context: CallerContext = CallerContextHolder.get()
            params = {"sub_path": encode_string(sub_path)}

            resp = self.rest_client.get(
                self.format_file_location_request_path(full_namespace, ident.name()),
                params=params,
                headers=(
                    caller_context.context() if caller_context is not None else None
                ),
                error_handler=FILESET_ERROR_HANDLER,
            )
            file_location_resp = FileLocationResponse.from_json(
                resp.body, infer_missing=True
            )
            file_location_resp.validate()

            return file_location_resp.file_location()
        finally:
            CallerContextHolder.remove()

    @staticmethod
    def check_fileset_namespace(namespace: Namespace):
        Namespace.check(
            namespace is not None and namespace.length() == 1,
            f"Fileset namespace must be non-null and have 1 level, the input namespace is {namespace}",
        )

    @staticmethod
    def check_fileset_name_identifier(ident: NameIdentifier):
        NameIdentifier.check(ident is not None, "NameIdentifier must not be None")
        NameIdentifier.check(
            ident.name() is not None and len(ident.name()) != 0,
            "NameIdentifier name must not be empty",
        )
        FilesetCatalog.check_fileset_namespace(ident.namespace())

    def _get_fileset_full_namespace(self, fileset_namespace: Namespace) -> Namespace:
        return Namespace.of(
            self._catalog_namespace.level(0), self.name(), fileset_namespace.level(0)
        )

    @staticmethod
    def format_fileset_request_path(namespace: Namespace) -> str:
        schema_ns = Namespace.of(namespace.level(0), namespace.level(1))
        return f"{BaseSchemaCatalog.format_schema_request_path(schema_ns)}/{encode_string(namespace.level(2))}/filesets"

    @staticmethod
    def format_file_location_request_path(namespace: Namespace, name: str) -> str:
        schema_ns = Namespace.of(namespace.level(0), namespace.level(1))
        return (
            f"{BaseSchemaCatalog.format_schema_request_path(schema_ns)}/{encode_string(namespace.level(2))}"
            f"/filesets/{encode_string(name)}/location"
        )

    @staticmethod
    def to_fileset_update_request(change: FilesetChange):
        if isinstance(change, FilesetChange.RenameFileset):
            return FilesetUpdateRequest.RenameFilesetRequest(change.new_name())
        if isinstance(change, FilesetChange.UpdateFilesetComment):
            return FilesetUpdateRequest.UpdateFilesetCommentRequest(
                change.new_comment()
            )
        if isinstance(change, FilesetChange.SetProperty):
            return FilesetUpdateRequest.SetFilesetPropertyRequest(
                change.property(), change.value()
            )
        if isinstance(change, FilesetChange.RemoveProperty):
            return FilesetUpdateRequest.RemoveFilesetPropertyRequest(change.property())
        if isinstance(change, FilesetChange.RemoveComment):
            return FilesetUpdateRequest.UpdateFilesetCommentRequest(None)
        raise ValueError(f"Unknown change type: {type(change).__name__}")

    def support_credentials(self) -> SupportsCredentials:
        return self

    def get_credentials(self) -> List[Credential]:
        return self._object_credential_operations.get_credentials()
