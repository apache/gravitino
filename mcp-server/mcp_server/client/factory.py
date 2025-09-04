# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from mcp_server.client.plain.plain_rest_client_operation import (
    PlainRESTClientOperation,
)


class RESTClientFactory:
    """
    Factory for creating and configuring Gravitino rest client instances.
    """

    _rest_client_class = PlainRESTClientOperation

    @classmethod
    def create_rest_client(
        cls, metalake_name: str, uri: str
    ) -> "PlainRESTClientOperation":
        """
        Create a new rest client instance with the specified parameters.

        Args:
            metalake_name: Name of the metalake
            uri: URI of the Gravitino server endpoint

        Returns:
            New instance of the configured rest client class
        """
        return cls._rest_client_class(metalake_name, uri)

    @classmethod
    def set_rest_client(cls, rest_client_class: type) -> None:
        """
        Configure the factory to use a different rest client class (primarily for testing).

        Args:
            rest_client_class: Class to use for future rest creations
        """
        cls._rest_client_class = rest_client_class
