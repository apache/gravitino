"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

from gravitino.utils.http_client import HTTPClient, unpack
from gravitino.typing import JSON_ro
from gravitino.constants import TIMEOUT


class _Service:
    def __init__(
        self,
        url: str,
        timeout: int = TIMEOUT,
    ) -> None:
        self.http_client = HTTPClient(url, timeout=timeout)

    @unpack("version")
    def get_version(self) -> JSON_ro:
        return self.http_client.get("/version")

    @unpack("metalakes")
    def get_metalakes(self) -> JSON_ro:
        return self.http_client.get("/metalakes")

    @unpack("metalake")
    def get_metalake(self, metalake: str) -> JSON_ro:
        return self.http_client.get(f"/metalakes/{metalake}")

    @unpack("identifiers")
    def get_catalogs(self, metalake: str) -> JSON_ro:
        return self.http_client.get(f"/metalakes/{metalake}/catalogs/")

    @unpack("catalog")
    def get_catalog(self, metalake: str, catalog: str) -> JSON_ro:
        return self.http_client.get(f"/metalakes/{metalake}/catalogs/{catalog}")

    @unpack("identifiers")
    def get_schemas(self, metalake: str, catalog: str) -> JSON_ro:
        return self.http_client.get(f"/metalakes/{metalake}/catalogs/{catalog}/schemas")

    @unpack("schema")
    def get_schema(self, metalake: str, catalog: str, schema: str) -> JSON_ro:
        return self.http_client.get(
            f"/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}"
        )

    @unpack("identifiers")
    def get_tables(self, metalake: str, catalog: str, schema: str) -> JSON_ro:
        return self.http_client.get(
            f"/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/tables"
        )

    @unpack("table")
    def get_table(
        self, metalake: str, catalog: str, schema: str, table: str
    ) -> JSON_ro:
        return self.http_client.get(
            f"/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/tables/{table}"
        )

    @unpack("names")
    def get_partitions(
        self, metalake: str, catalog: str, schema: str, table: str
    ) -> JSON_ro:
        return self.http_client.get(
            f"/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/tables/{table}/partitions"
        )


service = {}


def initialize_service(url: str, timeout: int = TIMEOUT):
    global service
    if not service:
        service["service"] = _Service(url, timeout)
