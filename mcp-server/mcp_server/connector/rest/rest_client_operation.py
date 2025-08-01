import httpx
from connector import CatalogOperation, GravitinoOperation
from connector.rest.rest_client_catalog_operation import \
    RESTClientCatalogOperation


class RESTClientOperation(GravitinoOperation):
    def __init__(self, metalake_name: str, uri: str):
        self.metalake_name = metalake_name
        self.rest_client = httpx.Client(base_url=uri)

    def as_catalog_operation(self) -> CatalogOperation:
        return RESTClientCatalogOperation(
            metalake_name=self.metalake_name, rest_client=self.rest_client
        )
