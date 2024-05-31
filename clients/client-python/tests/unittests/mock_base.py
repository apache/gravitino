"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

from gravitino import GravitinoMetalake, Catalog, Fileset
from gravitino.catalog.fileset_catalog import FilesetCatalog
from gravitino.dto.fileset_dto import FilesetDTO
from gravitino.dto.audit_dto import AuditDTO
from gravitino.dto.metalake_dto import MetalakeDTO

from unittest.mock import patch


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
    catalog = FilesetCatalog(
        name="fileset_catalog",
        catalog_type=Catalog.Type.FILESET,
        provider="hadoop",
        comment="this is test",
        properties={"k": "v"},
        audit=audit_dto,
        rest_client=None,
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
    class Wrapper(cls):
        pass

    return Wrapper
