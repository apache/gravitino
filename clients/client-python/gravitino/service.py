"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

from gravitino.utils import HTTPClient
from gravitino.typing import JSON_ro
from gravitino.constants import TIMEOUT


class Service:
    def __init__(
        self,
        url: str,
        timeout: int = TIMEOUT,
    ) -> None:
        self.base_url = url
        self.http_client = HTTPClient(timeout=timeout)

    def get_version(self) -> JSON_ro:
        return self.http_client.get(f"{self.base_url}/version")

    def get_metalakes(self) -> JSON_ro:
        return self.http_client.get(f"{self.base_url}/metalakes")

    def get_metalake(self, metalake: str) -> JSON_ro:
        return self.http_client.get(f"{self.base_url}/metalakes/{metalake}")

    def get_catalogs(self, metalake: str) -> JSON_ro:
        return self.http_client.get(f"{self.base_url}/metalakes/{metalake}/catalogs/")

    def get_catalog(self, metalake: str, catalog: str) -> JSON_ro:
        return self.http_client.get(
            f"{self.base_url}/metalakes/{metalake}/catalogs/{catalog}"
        )

    def get_schemas(self, metalake: str, catalog: str) -> JSON_ro:
        return self.http_client.get(
            f"{self.base_url}/metalakes/{metalake}/catalogs/{catalog}/schemas"
        )

    def get_schema(self, metalake: str, catalog: str, schema: str) -> JSON_ro:
        return self.http_client.get(
            f"{self.base_url}/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}"
        )

    def get_tables(self, metalake: str, catalog: str, schema: str) -> JSON_ro:
        return self.http_client.get(
            f"{self.base_url}/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/tables"
        )

    def get_table(self, metalake: str, catalog: str, schema: str, table: str) -> JSON_ro:
        return self.http_client.get(
            f"{self.base_url}/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/tables/{table}"
        )

    def get_partitions(self, metalake: str, catalog: str, schema: str, table: str) -> JSON_ro:
        return self.http_client.get(
            f"{self.base_url}/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/tables/{table}/partitions"
        )
