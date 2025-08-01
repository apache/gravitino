import logging

from connector import CatalogOperation
from connector.rest.utils import get_json_from_response
from httpx import Client


class RESTClientCatalogOperation(CatalogOperation):
    def __init__(self, metalake_name: str, rest_client: Client):
        self.metalake_name = metalake_name
        self.rest_client = rest_client

    def get_list_of_catalogs(self) -> str:
        response = self.rest_client.get(
            f"/api/metalakes/{self.metalake_name}/catalogs?details=true"
        )
        return get_json_from_response(response)
