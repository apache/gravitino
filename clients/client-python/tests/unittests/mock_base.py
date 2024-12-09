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
from gravitino.catalog.fileset_catalog import FilesetCatalog
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
    return GravitinoMetalake(metalake_dto)


def mock_load_fileset_catalog():
    audit_dto = AuditDTO(
        _creator="test",
        _create_time="2022-01-01T00:00:00Z",
        _last_modifier="test",
        _last_modified_time="2024-04-05T10:10:35.218Z",
    )

    namespace = Namespace.of("metalake_demo")

    catalog = FilesetCatalog(
        namespace=namespace,
        name="fileset_catalog",
        catalog_type=Catalog.Type.FILESET,
        provider="hadoop",
        comment="this is test",
        properties={"k": "v"},
        audit=audit_dto,
        rest_client=HTTPClient("http://localhost:9090", is_debug=True),
    )
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
        _properties={"k": "v"},
        _storage_location=location,
        _audit=audit_dto,
    )
    return fileset


def mock_data(cls):
    @patch(
        "gravitino.client.gravitino_client_base.GravitinoClientBase.load_metalake",
        return_value=mock_load_metalake(),
    )
    @patch(
        "gravitino.client.gravitino_metalake.GravitinoMetalake.load_catalog",
        return_value=mock_load_fileset_catalog(),
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
