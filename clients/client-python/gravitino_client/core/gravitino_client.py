"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""
import requests
from requests.exceptions import HTTPError
from gravitino_client.core.dto import VersionDTO


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

    def getVersion(self) -> VersionDTO:
        """
        Retrieves the version information from the Gravitino API.

        This method makes a GET request to the Gravitino API and extracts the version information from the response,
        wrapping it into a VersionDTO object.

        Returns:
            VersionDTO: An object containing the version details, including version, compile date, and git commit hash.

        Raises:
            HTTPError: An error from the requests library if the HTTP request returned an unsuccessful status code.
        """
        try:
            response = requests.get(f"{self.base_url}/api/version")
            response.raise_for_status()
            version_data = response.json()
            version_info = version_data.get("version")

            return VersionDTO(
                version=version_info['version'],
                compile_date=version_info['compileDate'],
                git_commit=version_info['gitCommit']
            )
        except HTTPError as e:
            raise HTTPError(f"Failed to retrieve version information: {e}")
