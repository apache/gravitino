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
import typing as tp
from contextlib import contextmanager
from http.client import HTTPResponse
from unittest.mock import MagicMock, Mock, patch

from gravitino import Catalog, Fileset, GravitinoMetalake
from gravitino.api.tag.tag_change import TagChange
from gravitino.client.fileset_catalog import FilesetCatalog
from gravitino.client.generic_fileset import GenericFileset
from gravitino.client.generic_model_catalog import GenericModelCatalog
from gravitino.dto.audit_dto import AuditDTO
from gravitino.dto.fileset_dto import FilesetDTO
from gravitino.dto.metalake_dto import MetalakeDTO
from gravitino.dto.schema_dto import SchemaDTO
from gravitino.dto.tag_dto import TagDTO
from gravitino.namespace import Namespace
from gravitino.utils import Response
from gravitino.utils.http_client import HTTPClient


def build_tag_dto(
    name: str = "tagA",
    comment: str = "commentA",
    properties: tp.Optional[dict[str, str]] = None,
) -> TagDTO:
    if properties is None:
        properties = {
            "key1": "value1",
            "key2": "value2",
        }

    return (
        TagDTO.builder()
        .name(name)
        .comment(comment)
        .properties(properties)
        .audit_info(build_audit_info())
        .inherited(False)
        .build()
    )


def build_audit_info() -> AuditDTO:
    return AuditDTO(
        _creator="test",
        _create_time="2022-01-01T00:00:00Z",
        _last_modifier="test",
        _last_modified_time="2024-04-05T10:10:35.218Z",
    )


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


def mock_load_schema(name: str):
    audit_dto = AuditDTO(
        _creator="test",
        _create_time="2022-01-01T00:00:00Z",
        _last_modifier="test",
        _last_modified_time="2024-04-05T10:10:35.218Z",
    )
    return SchemaDTO(
        _name=name,
        _comment="this is schema test",
        _properties={"schema-prop": "schema-val"},
        _audit=audit_dto,
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
        "gravitino.client.fileset_catalog.FilesetCatalog.load_schema",
        side_effect=mock_load_schema,
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


class MockTagRepo:
    def __init__(self) -> None:
        self.tag_store = {
            "tagA": build_tag_dto("tagA", "mock tag A"),
            "tagB": build_tag_dto("tagB", "mock tag B"),
        }

    def mock_list_tags(self) -> list[str]:
        return list(self.tag_store.keys())

    def mock_list_tags_info(self) -> list[TagDTO]:
        return list(self.tag_store.values())

    def mock_get_tag(self, tag_name: str) -> TagDTO:
        if tag_name not in self.tag_store:
            raise ValueError(f"Tag {tag_name} does not exist")
        return self.tag_store[tag_name]

    def mock_create_tag(
        self,
        tag_name: str,
        comment: str = "",
        properties=None,
    ) -> TagDTO:
        if tag_name in self.tag_store:
            raise ValueError(f"Tag {tag_name} already exists")
        self.tag_store[tag_name] = build_tag_dto(tag_name, comment, properties)
        return self.tag_store[tag_name]

    def mock_alter_tag(self, tag_name: str, *changes) -> TagDTO:
        if tag_name not in self.tag_store:
            raise ValueError(f"Tag {tag_name} does not exist")

        for change in changes:
            current_tag_obj = self.tag_store[tag_name]

            if isinstance(change, TagChange.RenameTag):
                self.tag_store[change.new_name] = build_tag_dto(
                    change.new_name,
                    current_tag_obj.comment(),
                    dict(current_tag_obj.properties()),
                )
                del self.tag_store[tag_name]
                tag_name = change.new_name

            elif isinstance(change, TagChange.ChangeComment):
                self.tag_store[tag_name] = build_tag_dto(
                    current_tag_obj.name(),
                    change.new_comment,
                    dict(current_tag_obj.properties()),
                )

            elif isinstance(change, TagChange.RemoveProperty):
                new_properties = dict(current_tag_obj.properties())
                new_properties.pop(change.removed_property, None)

                self.tag_store[tag_name] = build_tag_dto(
                    current_tag_obj.name(),
                    current_tag_obj.comment(),
                    new_properties,
                )

            elif isinstance(change, TagChange.SetProperty):
                new_properties = dict(current_tag_obj.properties())
                new_properties[change.name] = change.value

                self.tag_store[tag_name] = build_tag_dto(
                    current_tag_obj.name(),
                    current_tag_obj.comment(),
                    new_properties,
                )

            else:
                raise ValueError(f"Unknown tag change type: {change}")

        return self.tag_store[tag_name]

    def mock_delete_tag(self, tag_name: str) -> bool:
        if tag_name not in self.tag_store:
            return False
        del self.tag_store[tag_name]
        return True


@contextmanager
def mock_tag_methods():
    repo = MockTagRepo()

    with patch.multiple(
        GravitinoMetalake,
        list_tags=MagicMock(side_effect=repo.mock_list_tags),
        list_tags_info=MagicMock(side_effect=repo.mock_list_tags_info),
        get_tag=MagicMock(side_effect=repo.mock_get_tag),
        create_tag=MagicMock(side_effect=repo.mock_create_tag),
        alter_tag=MagicMock(side_effect=repo.mock_alter_tag),
        delete_tag=MagicMock(side_effect=repo.mock_delete_tag),
    ) as mocks:
        yield mocks, repo


def mock_http_response(json_str: str) -> Response:
    mock_http_resp = Mock(HTTPResponse)
    mock_http_resp.getcode.return_value = 200
    mock_http_resp.read.return_value = json_str
    mock_http_resp.info.return_value = None
    mock_http_resp.url = None
    mock_resp = Response(mock_http_resp)
    return mock_resp
