"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

from gravitino.api.catalog import Catalog
from gravitino.api.catalog_change import CatalogChange
from gravitino.catalog.fileset_catalog import FilesetCatalog
from gravitino.dto.catalog_dto import CatalogDTO
from gravitino.dto.requests.catalog_update_request import CatalogUpdateRequest
from gravitino.dto.requests.metalake_update_request import MetalakeUpdateRequest
from gravitino.api.metalake_change import MetalakeChange
from gravitino.utils import HTTPClient


class DTOConverters:
    """Utility class for converting between DTOs and domain objects."""

    @staticmethod
    def to_metalake_update_request(change: MetalakeChange) -> object:
        # Assuming MetalakeUpdateRequest has similar nested class structure for requests
        if isinstance(change, MetalakeChange.RenameMetalake):
            return MetalakeUpdateRequest.RenameMetalakeRequest(change.new_name())
        if isinstance(change, MetalakeChange.UpdateMetalakeComment):
            return MetalakeUpdateRequest.UpdateMetalakeCommentRequest(
                change.new_comment()
            )
        if isinstance(change, MetalakeChange.SetProperty):
            return MetalakeUpdateRequest.SetMetalakePropertyRequest(
                change.property(), change.value()
            )
        if isinstance(change, MetalakeChange.RemoveProperty):
            return MetalakeUpdateRequest.RemoveMetalakePropertyRequest(
                change.property()
            )

        raise ValueError(f"Unknown change type: {type(change).__name__}")

    @staticmethod
    def to_catalog(catalog: CatalogDTO, client: HTTPClient):
        if catalog.type() == Catalog.Type.FILESET:
            return FilesetCatalog(
                name=catalog.name(),
                type=catalog.type(),
                provider=catalog.provider(),
                comment=catalog.comment(),
                properties=catalog.properties(),
                audit=catalog.audit_info(),
                rest_client=client,
            )

        raise NotImplementedError("Unsupported catalog type: " + str(catalog.type()))

    @staticmethod
    def to_catalog_update_request(change: CatalogChange):
        if isinstance(change, CatalogChange.RenameCatalog):
            return CatalogUpdateRequest.RenameCatalogRequest(change.new_name)
        if isinstance(change, CatalogChange.UpdateCatalogComment):
            return CatalogUpdateRequest.UpdateCatalogCommentRequest(change.new_comment)
        if isinstance(change, CatalogChange.SetProperty):
            # TODO
            # pylint: disable=too-many-function-args
            return CatalogUpdateRequest.SetCatalogPropertyRequest(
                change.property(), change.value()
            )
        if isinstance(change, CatalogChange.RemoveProperty):
            return CatalogUpdateRequest.RemoveCatalogPropertyRequest(change._property)

        raise ValueError(f"Unknown change type: {type(change).__name__}")
