"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""
from typing import Dict

from gravitino.dto.audit_dto import AuditDTO
from gravitino.dto.metalake_dto import MetalakeDTO
from gravitino.utils import HTTPClient


class GravitinoMetalake(MetalakeDTO):
    """
    Gravitino Metalake is the top-level metadata repository for users. It contains a list of catalogs
    as sub-level metadata collections. With {@link GravitinoMetalake}, users can list, create, load,
    alter and drop a catalog with specified identifier.
    """
    restClient: HTTPClient

    def __init__(self, name: str = None, comment: str = None, properties: Dict[str, str] = None, audit: AuditDTO = None,
                 rest_client: HTTPClient = None):
        super().__init__(name=name, comment=comment, properties=properties, audit=audit)
        self.restClient = rest_client

    @classmethod
    def build(cls, metalake: MetalakeDTO = None, client: HTTPClient = None):
        return cls(name=metalake.name, comment=metalake.comment, properties=metalake.properties,
                   audit=metalake.audit, rest_client=client)
