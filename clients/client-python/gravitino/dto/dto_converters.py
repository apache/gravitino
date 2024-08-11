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

from gravitino.api.catalog import Catalog
from gravitino.api.catalog_change import CatalogChange
from gravitino.catalog.fileset_catalog import FilesetCatalog
from gravitino.dto.catalog_dto import CatalogDTO
from gravitino.dto.requests.catalog_update_request import CatalogUpdateRequest
from gravitino.dto.requests.metalake_update_request import MetalakeUpdateRequest
from gravitino.api.metalake_change import MetalakeChange
from gravitino.utils import HTTPClient
from gravitino.namespace import Namespace


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
    def to_catalog(metalake: str, catalog: CatalogDTO, client: HTTPClient):
        namespace = Namespace.of(metalake)
        if catalog.type() == Catalog.Type.FILESET:
            return FilesetCatalog(
                namespace=namespace,
                name=catalog.name(),
                catalog_type=catalog.type(),
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
            return CatalogUpdateRequest.RenameCatalogRequest(change.new_name())
        if isinstance(change, CatalogChange.UpdateCatalogComment):
            return CatalogUpdateRequest.UpdateCatalogCommentRequest(
                change.new_comment()
            )
        if isinstance(change, CatalogChange.SetProperty):
            # TODO
            # pylint: disable=too-many-function-args
            return CatalogUpdateRequest.SetCatalogPropertyRequest(
                change.property(), change.value()
            )
        if isinstance(change, CatalogChange.RemoveProperty):
            return CatalogUpdateRequest.RemoveCatalogPropertyRequest(
                change.get_property()
            )

        raise ValueError(f"Unknown change type: {type(change).__name__}")
