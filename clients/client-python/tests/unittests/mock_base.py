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

import json
from unittest.mock import patch

from gravitino import GravitinoMetalake, Catalog, Fileset
from gravitino.client.fileset_catalog import FilesetCatalog
from gravitino.client.generic_fileset import GenericFileset
from gravitino.client.generic_model_catalog import GenericModelCatalog
from gravitino.dto.fileset_dto import FilesetDTO
from gravitino.dto.audit_dto import AuditDTO
from gravitino.dto.metalake_dto import MetalakeDTO
from gravitino.namespace import Namespace
from gravitino.utils.http_client import HTTPClient


def mock_load_metalake():
    audit_dto = AuditDTO(
        _creator="test",
        _create_time="2022-01-01T00:00:00Z",
        _last_modifier="test",
        _last_modified_time="2024-04-05T10:10:35.218Z",
    )
    metalake_dto = MetalakeDTO(
        _name="metalake_demo",
        _comment="this is test",
        _properties={"k": "v"},
        _audit=audit_dto,
    )
    return GravitinoMetalake(
        metalake_dto, HTTPClient("http://localhost:9090", is_debug=True)
    )


def mock_load_catalog(name: str):
    audit_dto = AuditDTO(
        _creator="test",
        _create_time="2022-01-01T00:00:00Z",
        _last_modifier="test",
        _last_modified_time="2024-04-05T10:10:35.218Z",
    )

    namespace = Namespace.of("metalake_demo")

    catalog = None
    if name == "fileset_catalog":
        catalog = FilesetCatalog(
            namespace=namespace,
            name=name,
            catalog_type=Catalog.Type.FILESET,
            provider="hadoop",
            comment="this is test",
            properties={"k": "v"},
            audit=audit_dto,
            rest_client=HTTPClient("http://localhost:9090", is_debug=True),
        )
    elif name == "model_catalog":
        catalog = GenericModelCatalog(
            namespace=namespace,
            name=name,
            catalog_type=Catalog.Type.MODEL,
            provider="hadoop",
            comment="this is test",
            properties={"k": "v"},
            audit=audit_dto,
            rest_client=HTTPClient("http://localhost:9090", is_debug=True),
        )
    else:
        raise ValueError(f"Unknown catalog name: {name}")

    return catalog


def mock_load_fileset(name: str, location: str):
    audit_dto = AuditDTO(
        _creator="test",
        _create_time="2022-01-01T00:00:00Z",
        _last_modifier="test",
        _last_modified_time="2024-04-05T10:10:35.218Z",
    )
    fileset = FilesetDTO(
        _name=name,
        _type=Fileset.Type.MANAGED,
        _comment="this is test",
        _properties={
            "k": "v",
            Fileset.PROPERTY_DEFAULT_LOCATION_NAME: Fileset.LOCATION_NAME_UNKNOWN,
        },
        _storage_locations={Fileset.LOCATION_NAME_UNKNOWN: location},
        _audit=audit_dto,
    )
    return GenericFileset(
        fileset, None, Namespace.of("metalake_demo", "fileset_catalog", "tmp")
    )


def mock_data(cls):
    @patch(
        "gravitino.client.gravitino_client_base.GravitinoClientBase.load_metalake",
        return_value=mock_load_metalake(),
    )
    @patch(
        "gravitino.client.gravitino_metalake.GravitinoMetalake.load_catalog",
        side_effect=mock_load_catalog,
    )
    @patch(
        "gravitino.client.fileset_catalog.FilesetCatalog.load_fileset",
        return_value=mock_load_fileset("fileset", ""),
    )
    @patch(
        "gravitino.client.gravitino_client_base.GravitinoClientBase.check_version",
        return_value=True,
    )
    class Wrapper(cls):
        pass

    return Wrapper


def mock_name_identifier_json(name, namespace):
    return json.dumps({"name": name, "namespace": namespace}).encode("utf-8")
