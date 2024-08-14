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

import logging
from typing import Dict, List

from gravitino.api.catalog import Catalog
from gravitino.api.schema import Schema
from gravitino.api.schema_change import SchemaChange
from gravitino.api.supports_schemas import SupportsSchemas
from gravitino.dto.audit_dto import AuditDTO
from gravitino.dto.catalog_dto import CatalogDTO
from gravitino.dto.requests.schema_create_request import SchemaCreateRequest
from gravitino.dto.requests.schema_update_request import SchemaUpdateRequest
from gravitino.dto.requests.schema_updates_request import SchemaUpdatesRequest
from gravitino.dto.responses.drop_response import DropResponse
from gravitino.dto.responses.entity_list_response import EntityListResponse
from gravitino.dto.responses.schema_response import SchemaResponse
from gravitino.exceptions.handlers.schema_error_handler import SCHEMA_ERROR_HANDLER
from gravitino.namespace import Namespace
from gravitino.utils import HTTPClient
from gravitino.rest.rest_utils import encode_string
from gravitino.exceptions.base import IllegalArgumentException

logger = logging.getLogger(__name__)


class BaseSchemaCatalog(CatalogDTO, SupportsSchemas):
    """
    BaseSchemaCatalog is the base abstract class for all the catalog with schema. It provides the
    common methods for managing schemas in a catalog. With BaseSchemaCatalog, users can list,
    create, load, alter and drop a schema with specified identifier.
    """

    # The REST client to send the requests.
    rest_client: HTTPClient

    # The namespace of current catalog, which is the metalake name.
    _catalog_namespace: Namespace

    def __init__(
        self,
        catalog_namespace: Namespace,
        name: str = None,
        catalog_type: Catalog.Type = Catalog.Type.UNSUPPORTED,
        provider: str = None,
        comment: str = None,
        properties: Dict[str, str] = None,
        audit: AuditDTO = None,
        rest_client: HTTPClient = None,
    ):
        super().__init__(
            _name=name,
            _type=catalog_type,
            _provider=provider,
            _comment=comment,
            _properties=properties,
            _audit=audit,
        )
        self.rest_client = rest_client
        self._catalog_namespace = catalog_namespace

        self.validate()

    def as_schemas(self):
        return self

    def list_schemas(self) -> List[str]:
        """List all the schemas under the given catalog namespace.

        Raises:
            NoSuchCatalogException if the catalog with specified namespace does not exist.

        Returns:
             A list of schema names under the given catalog namespace.
        """
        resp = self.rest_client.get(
            BaseSchemaCatalog.format_schema_request_path(self._schema_namespace()),
            error_handler=SCHEMA_ERROR_HANDLER,
        )
        entity_list_response = EntityListResponse.from_json(
            resp.body, infer_missing=True
        )
        entity_list_response.validate()

        return [ident.name() for ident in entity_list_response.identifiers()]

    def create_schema(
        self,
        schema_name: str = None,
        comment: str = None,
        properties: Dict[str, str] = None,
    ) -> Schema:
        """Create a new schema with specified identifier, comment and metadata.

        Args:
            schema_name: The name of the schema.
            comment: The comment of the schema.
            properties: The properties of the schema.

        Raises:
            NoSuchCatalogException if the catalog with specified namespace does not exist.
            SchemaAlreadyExistsException if the schema with specified identifier already exists.

        Returns:
             The created Schema.
        """
        req = SchemaCreateRequest(encode_string(schema_name), comment, properties)
        req.validate()

        resp = self.rest_client.post(
            BaseSchemaCatalog.format_schema_request_path(self._schema_namespace()),
            json=req,
            error_handler=SCHEMA_ERROR_HANDLER,
        )
        schema_response = SchemaResponse.from_json(resp.body, infer_missing=True)
        schema_response.validate()

        return schema_response.schema()

    def load_schema(self, schema_name: str) -> Schema:
        """Load the schema with specified identifier.

        Args:
            schema_name: The name of the schema.

        Raises:
            NoSuchSchemaException if the schema with specified identifier does not exist.

        Returns:
            The Schema with specified identifier.
        """
        resp = self.rest_client.get(
            BaseSchemaCatalog.format_schema_request_path(self._schema_namespace())
            + "/"
            + encode_string(schema_name),
            error_handler=SCHEMA_ERROR_HANDLER,
        )
        schema_response = SchemaResponse.from_json(resp.body, infer_missing=True)
        schema_response.validate()

        return schema_response.schema()

    def alter_schema(self, schema_name: str, *changes: SchemaChange) -> Schema:
        """Alter the schema with specified identifier by applying the changes.

        Args:
            schema_name: The name of the schema.
            changes: The metadata changes to apply.

        Raises:
            NoSuchSchemaException if the schema with specified identifier does not exist.

        Returns:
            The altered Schema.
        """
        reqs = [
            BaseSchemaCatalog.to_schema_update_request(change) for change in changes
        ]
        updates_request = SchemaUpdatesRequest(reqs)
        updates_request.validate()
        resp = self.rest_client.put(
            BaseSchemaCatalog.format_schema_request_path(self._schema_namespace())
            + "/"
            + encode_string(schema_name),
            updates_request,
            error_handler=SCHEMA_ERROR_HANDLER,
        )
        schema_response = SchemaResponse.from_json(resp.body, infer_missing=True)
        schema_response.validate()
        return schema_response.schema()

    def drop_schema(self, schema_name: str, cascade: bool) -> bool:
        """Drop the schema with specified identifier.

        Args:
            schema_name: The name of the schema.
            cascade: Whether to drop all the tables under the schema.

        Raises:
            NonEmptySchemaException if the schema is not empty and cascade is false.

        Returns:
             true if the schema is dropped successfully, false otherwise.
        """
        try:
            params = {"cascade": str(cascade)}
            resp = self.rest_client.delete(
                BaseSchemaCatalog.format_schema_request_path(self._schema_namespace())
                + "/"
                + encode_string(schema_name),
                params=params,
                error_handler=SCHEMA_ERROR_HANDLER,
            )
            drop_resp = DropResponse.from_json(resp.body, infer_missing=True)
            drop_resp.validate()
            return drop_resp.dropped()
        except Exception:
            logger.warning("Failed to drop schema %s", schema_name)
            return False

    def _schema_namespace(self) -> Namespace:
        return Namespace.of(self._catalog_namespace.level(0), self.name())

    @staticmethod
    def format_schema_request_path(ns: Namespace):
        return "api/metalakes/" + ns.level(0) + "/catalogs/" + ns.level(1) + "/schemas"

    @staticmethod
    def to_schema_update_request(change: SchemaChange):
        if isinstance(change, SchemaChange.SetProperty):
            return SchemaUpdateRequest.SetSchemaPropertyRequest(
                change.property(), change.value()
            )
        if isinstance(change, SchemaChange.RemoveProperty):
            return SchemaUpdateRequest.RemoveSchemaPropertyRequest(change.property())
        raise ValueError(f"Unknown change type: {type(change).__name__}")

    def validate(self):
        Namespace.check(
            self._catalog_namespace is not None
            and self._catalog_namespace.length() == 1,
            f"Catalog namespace must be non-null and have 1 level, the input namespace is {self._catalog_namespace}",
        )

        if self.rest_client is None:
            raise IllegalArgumentException("restClient must be set")
        if not self.name() or not self.name().strip():
            raise IllegalArgumentException("name must not be blank")
        if self.type() is None:
            raise IllegalArgumentException("type must not be None")
        if not self.provider() or not self.provider().strip():
            raise IllegalArgumentException("provider must not be blank")
        if self.audit_info() is None:
            raise IllegalArgumentException("audit must not be None")
