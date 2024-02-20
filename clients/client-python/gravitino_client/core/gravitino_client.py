import requests
from typing import Dict

class GravitinoClient:
    """
    Gravitino Client for interacting with the Gravitino API, allowing the client to list, load,
    create, and alter Metalakes.

    Attributes:
        base_url (str): The base URL of the Gravitino API to which the client will make requests.

    Args:
        base_url (str): The base URL for the Gravitino API.
    """

    def __init__(self, base_url):
        """
        Initializes a new instance of the GravitinoClient.

        Args:
            base_url (str): The base URL for the Gravitino API where the client will send requests.
        """
        self.base_url = base_url

    def getVersion(self) -> Dict[str, str]:
        """
        Retrieves the version information from the Gravitino API.

        This method makes a GET request to the Gravitino API and extracts the version information from the response.

        Returns:
            A dictionary containing the version details. For example:
            {
                'version': '0.3.2-SNAPSHOT',
                'compileDate': '25/01/2024 00:04:59',
                'gitCommit': 'cb7a604bf19b6f992f00529e938cdd1d37af0187'
            }

        Raises:
            HTTPError: An error from the requests library if the HTTP request returned an unsuccessful status code.
        """
        response = requests.get(f"{self.base_url}/api/version")
        response.raise_for_status()
        version_info = response.json()
        return version_info.get('version')
