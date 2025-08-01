import httpx
from mcp_server.connector import CatalogOperation, GravitinoOperation, \
    SchemaOperation, TableOperation
from mcp_server.connector.rest.rest_client_catalog_operation import \
    RESTClientCatalogOperation
from mcp_server.connector.rest.rest_client_schema_operation import \
    RESTClientSchemaOperation
from mcp_server.connector.rest.rest_client_table_operation import \
    RESTClientTableOperation


class RESTClientOperation(GravitinoOperation):
    def __init__(self, metalake_name: str, uri: str):
        self.metalake_name = metalake_name
        self.rest_client = httpx.Client(base_url=uri)

    def as_catalog_operation(self) -> CatalogOperation:
        return RESTClientCatalogOperation(
            metalake_name=self.metalake_name, rest_client=self.rest_client
        )

    def as_table_operation(self) -> TableOperation:
        return RESTClientTableOperation(
            metalake_name=self.metalake_name, rest_client=self.rest_client
        )

    def as_schema_operation(self) -> SchemaOperation:
        return RESTClientSchemaOperation(
            metalake_name=self.metalake_name, rest_client=self.rest_client
        )
