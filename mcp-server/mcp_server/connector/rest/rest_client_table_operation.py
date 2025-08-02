from mcp_server.connector import TableOperation
from httpx import Client
from mcp_server.connector.rest.utils import extract_content_from_response

class RESTClientTableOperation(TableOperation):

    def __init__(self, metalake_name: str, rest_client: Client):
        self.metalake_name = metalake_name
        self.rest_client = rest_client

    def get_list_of_tables(self, catalog_name: str, schema_name: str):
        response = self.rest_client.get(
            f"/api/metalakes/{self.metalake_name}/catalogs/{catalog_name}/schemas/{schema_name}/tables"
        )
        return extract_content_from_response(response, "tables", [])

    def load_table(self, catalog_name: str, schema_name: str, table_name: str):
        response = self.rest_client.get(
            f"/api/metalakes/{self.metalake_name}/catalogs/{catalog_name}/schemas/{schema_name}/tables/{table_name}"
        )
        return extract_content_from_response(response, "table", {})
