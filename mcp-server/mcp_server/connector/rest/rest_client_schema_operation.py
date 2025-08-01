from connector import GravitinoSchemaOperation
from httpx import Client

from .utils import get_json_from_response


class RESTClientSchemaOperation(GravitinoSchemaOperation):
    def __init__(self, metalake_name: str, rest_client: Client):
        self.metalake_name = metalake_name
        self.rest_client = rest_client

    def get_list_of_schmas(self, catalog_name: str):
        response = self.rest_client.get(
            f"/api/metalakes/{self.metalake_name}/catalogs/{catalog_name}/schemas"
        )
        return get_json_from_response(response)
