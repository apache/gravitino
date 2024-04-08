"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""
import logging
from dataclasses import dataclass
from typing import List, Dict

from gravitino.api.catalog import Catalog
from gravitino.api.fileset import Fileset
from gravitino.api.fileset_change import FilesetChange
from gravitino.catalog.base_schema_catalog import BaseSchemaCatalog
from gravitino.dto.audit_dto import AuditDTO
from gravitino.dto.requests.fileset_create_request import FilesetCreateRequest
from gravitino.dto.requests.fileset_update_request import FilesetUpdateRequest
from gravitino.dto.requests.fileset_updates_request import FilesetUpdatesRequest
from gravitino.dto.responses.drop_response import DropResponse
from gravitino.dto.responses.entity_list_response import EntityListResponse
from gravitino.dto.responses.fileset_response import FilesetResponse
from gravitino.name_identifier import NameIdentifier
from gravitino.namespace import Namespace
from gravitino.utils import HTTPClient

logger = logging.getLogger(__name__)


# @dataclass
class FilesetCatalog(BaseSchemaCatalog):
    """Fileset catalog is a catalog implementation that supports fileset like metadata operations, for
    example, schemas and filesets list, creation, update and deletion. A Fileset catalog is under the metalake.
    """

    def __init__(self, name: str = None, type: Catalog.Type = Catalog.Type.UNSUPPORTED,
                 provider: str = None, comment: str = None, properties: Dict[str, str] = None,
                 audit: AuditDTO = None, rest_client: HTTPClient = None):

        # assert rest_client is not None, "restClient must be set"
        # assert name.strip(), "name must not be blank"
        # assert type is not None, "type must not be null"
        # assert name.strip(), "provider must not be blank"
        # assert audit is not None, "audit must not be null"

        super().__init__(name, type, provider, comment, properties, audit, rest_client)

    def as_fileset_catalog(self):
        return self

    def list_filesets(self, namespace: Namespace) -> List[NameIdentifier]:
        """List the filesets in a schema namespace from the catalog.

        Args:
            namespace A schema namespace.

        Raises:
            NoSuchSchemaException If the schema does not exist.

        Returns:
            An array of fileset identifiers in the namespace.
        """
        Namespace.check_fileset(namespace)

        resp = self.rest_client.get(
            self.format_fileset_request_path(namespace)
        )
        entity_list_resp = EntityListResponse.from_json(resp.body, infer_missing=True)
        entity_list_resp.validate()

        return entity_list_resp.idents

    def load_fileset(self, ident) -> Fileset:
        """Load fileset metadata by {@link NameIdentifier} from the catalog.

        Args:
            ident: A fileset identifier.

        Raises:
            NoSuchFilesetException If the fileset does not exist.

        Returns:
            The fileset metadata.
        """
        NameIdentifier.check_fileset(ident)

        resp = self.rest_client.get(f"{self.format_fileset_request_path(ident.namespace())}/{ident.name()}")
        fileset_resp = FilesetResponse.from_json(resp.body, infer_missing=True)
        fileset_resp.validate()

        return fileset_resp.fileset

    def create_fileset(self, ident: NameIdentifier, comment: str, type: Catalog.Type,
                       storage_location: str, properties: Dict[str, str]) -> Fileset:
        """Create a fileset metadata in the catalog.

        If the type of the fileset object is "MANAGED", the underlying storageLocation can be null,
        and Gravitino will manage the storage location based on the location of the schema.

        If the type of the fileset object is "EXTERNAL", the underlying storageLocation must be set.

        Args:
            ident: A fileset identifier.
            comment: The comment of the fileset.
            type: The type of the fileset.
            storage_location: The storage location of the fileset.
            properties: The properties of the fileset.

        Raises:
            NoSuchSchemaException If the schema does not exist.
            FilesetAlreadyExistsException If the fileset already exists.

        Returns:
            The created fileset metadata
        """
        NameIdentifier.check_fileset(ident)

        req = FilesetCreateRequest(name=ident.name(), comment=comment, type=type,
                                   storage_location=storage_location, properties=properties)

        resp = self.rest_client.post(self.format_fileset_request_path(ident.namespace()), req)
        fileset_resp = FilesetResponse.from_json(resp.body, infer_missing=True)
        fileset_resp.validate()

        return fileset_resp.fileset

    def alter_fileset(self, ident, *changes) -> Fileset:
        """Update a fileset metadata in the catalog.

        Args:
            ident: A fileset identifier.
            changes: The changes to apply to the fileset.

        Args:
            IllegalArgumentException If the changes are invalid.
            NoSuchFilesetException If the fileset does not exist.

        Returns:
            The updated fileset metadata.
        """
        NameIdentifier.check_fileset(ident)

        updates = [FilesetCatalog.to_fileset_update_request(change) for change in changes]
        req = FilesetUpdatesRequest(updates)
        req.validate()

        resp = self.rest_client.put(f"{self.format_fileset_request_path(ident.namespace())}/{ident.name()}", req)
        fileset_resp = FilesetResponse.from_json(resp.body, infer_missing=True)
        fileset_resp.validate()

        return fileset_resp.fileset

    def drop_fileset(self, ident: NameIdentifier) -> bool:
        """Drop a fileset from the catalog.

        The underlying files will be deleted if this fileset type is managed, otherwise, only the
        metadata will be dropped.

        Args:
             ident: A fileset identifier.

        Returns:
             true If the fileset is dropped, false the fileset did not exist.
        """
        try:
            NameIdentifier.check_fileset(ident)

            resp = self.rest_client.delete(
                f"{self.format_fileset_request_path(ident.namespace())}/{ident.name()}",
            )
            drop_resp = DropResponse.from_json(resp.body, infer_missing=True)
            drop_resp.validate()

            return drop_resp.dropped()
        except Exception as e:
            logger.warning(f"Failed to drop fileset {ident}: {e}")
            return False

    @staticmethod
    def format_fileset_request_path(namespace: Namespace) -> str:
        schema_ns = Namespace.of(namespace.level(0), namespace.level(1))
        return f"{BaseSchemaCatalog.format_schema_request_path(schema_ns)}/{namespace.level(2)}/filesets"

    @staticmethod
    def to_fileset_update_request(change: FilesetChange):
        if isinstance(change, FilesetChange.RenameFileset):
            return FilesetUpdateRequest.RenameFilesetRequest(change.new_name)
        elif isinstance(change, FilesetChange.UpdateFilesetComment):
            return FilesetUpdateRequest.UpdateFilesetCommentRequest(change.new_comment)
        elif isinstance(change, FilesetChange.SetProperty):
            return FilesetUpdateRequest.SetFilesetPropertyRequest(change.property, change.value)
        elif isinstance(change, FilesetChange.RemoveProperty):
            return FilesetUpdateRequest.RemoveFilesetPropertyRequest(change.property)
        else:
            raise ValueError(f"Unknown change type: {type(change).__name__}")
