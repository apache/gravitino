"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

import re

from gravitino.constants import TIMEOUT
from gravitino.service import initialize_service, service
from gravitino.typing import JSON_ro


class MetaLake:
    def __init__(self, payload: JSON_ro):
        self.name = payload.get("name")
        self.payload = payload
        self.service = service["service"]
        self.catalogs = self.service.list_catalogs(self.name)

    def __repr__(self):
        return f"MetaLake<{self.name}>"

    def __getattr__(self, catalog_name):
        if catalog_name in dir(self):
            return Catalog(self.name, catalog_name)

    def __dir__(self):
        return [catalog["name"] for catalog in self.catalogs]

    def __contains__(self, item):
        return item in dir(self)


class Catalog:
    def __init__(self, metalake_name: str, catalog_name: str):
        self.metalake_name = metalake_name
        self.catalog_name = catalog_name
        self.name = catalog_name
        self.service = service["service"]
        self.schemas = self.service.list_schemas(metalake_name, catalog_name)

    def __repr__(self):
        return f"Catalog<{self.name}>"

    def __getattr__(self, schema_name):
        if schema_name in dir(self):
            return Schema(self.metalake_name, self.catalog_name, schema_name)

    def __dir__(self):
        return [schema["name"] for schema in self.schemas]

    def __contains__(self, item):
        return item in dir(self)


class Schema:
    def __init__(self, metalake_name: str, catalog_name: str, schema_name: str):
        self.metalake_name = metalake_name
        self.catalog_name = catalog_name
        self.schema_name = schema_name
        self.name = schema_name
        self.service = service["service"]
        self.tables = self.service.list_tables(metalake_name, catalog_name, schema_name)

    def __repr__(self):
        return f"Schema<{self.name}>"

    def __getattr__(self, table_name):
        if table_name in dir(self):
            return Table(
                self.metalake_name, self.catalog_name, self.schema_name, table_name
            )

    def __dir__(self):
        return [table["name"] for table in self.tables]

    def __contains__(self, item):
        return item in dir(self)


class Table:
    def __init__(
        self, metalake_name: str, catalog_name: str, schema_name: str, table_name: str
    ):
        self.metalake_name = metalake_name
        self.catalog_name = catalog_name
        self.schema_name = schema_name
        self.table_name = table_name
        self.name = schema_name
        self.service = service["service"]

    def __repr__(self):
        return f"Table<{self.name}>"

    def info(self):
        return self.service.get_table(
            self.metalake_name, self.catalog_name, self.schema_name, self.table_name
        )


class GravitinoClient:
    def __init__(
        self,
        host: str,
        *,
        protocol: str = "http",
        port: int = 8090,
        prefix: str = "/api",
        timeout: int = TIMEOUT,
        debug: bool = False,
    ) -> None:
        if re.search(r"^https?:\/\/", host):
            _host = host.rstrip("/")
        else:
            _host = f"{protocol}://{host.rstrip('/')}"

        if not re.search(r"[0-9]{2,5}$", _host):
            _host = f"{_host}:{port}"

        _base_url = f"{_host}/{prefix.strip('/')}"
        initialize_service(_base_url, timeout)
        self.service = service["service"]
        self.debug = debug

    @classmethod
    def initialize_metalake(
        cls,
        host: str,
        metalake_name: str,
        *,
        protocol: str = "http",
        port: int = 8090,
        prefix: str = "/api",
        timeout: int = TIMEOUT,
        debug: bool = False,
    ) -> MetaLake:
        # keep in mind, all constructors should include same interface as __init__ function
        client = cls(
            host,
            protocol=protocol,
            port=port,
            prefix=prefix,
            timeout=timeout,
            debug=debug,
        )
        return client.get_metalake(metalake_name)

    @property
    def version(self):
        return self.service.get_version()

    def get_metalakes(self) -> [MetaLake]:
        return [MetaLake(metalake) for metalake in self.service.list_metalakes()]

    def get_metalake(self, metalake: str) -> MetaLake:
        return MetaLake(self.service.get_metalake(metalake))


def gravitino_metalake(
    host: str,
    metalake_name: str,
    *,
    protocol: str = "http",
    port: int = 8090,
    prefix: str = "/api",
    timeout: int = TIMEOUT,
    debug: bool = False,
) -> MetaLake:
    return GravitinoClient.initialize_metalake(
        host,
        metalake_name,
        protocol=protocol,
        port=port,
        prefix=prefix,
        timeout=timeout,
        debug=debug,
    )
