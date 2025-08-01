from connector import GravitinoTableOperation
from httpx import Client

from .utils import get_json_from_response


class RESTClientTableOperation(GravitinoTableOperation):

    def __init__(self, metalake_name: str, rest_client: Client):
        self.metalake_name = metalake_name
        self.rest_client = rest_client

    def get_list_of_tables(self, catalog_name: str, schema_name: str):
        response = self.rest_client.get(
            f"/api/metalakes/{self.metalake_name}/catalogs/{catalog_name}/schemas/{schema_name}/tables"
        )
        return get_json_from_response(response)

    def load_table(self, catalog_name: str, schema_name: str, table_name: str):
        response = self.rest_client.get(
            f"/api/metalakes/{self.metalake_name}/catalogs/{catalog_name}/schemas/{schema_name}/tables/{table_name}"
        )
        return get_json_from_response(response)
