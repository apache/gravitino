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

from abc import ABC, abstractmethod


class FilesetOperation(ABC):
    """
    Abstract base class for Gravitino fileset operations.
    """

    @abstractmethod
    async def list_of_filesets(
        self, catalog_name: str, schema_name: str
    ) -> str:
        """
        Retrieve the list of filesets within a specified catalog.

        Args:
            catalog_name: Name of the catalog
            schema_name: Name of the schema

        Returns:
            str: JSON-formatted string containing fileset list information
        """
        pass

    @abstractmethod
    async def load_fileset(
        self, catalog_name: str, schema_name: str, fileset_name: str
    ) -> str:
        """
        Load detailed information of a specific fileset.

        Args:
            catalog_name: Name of the catalog
            schema_name: Name of the schema
            fileset_name: Name of the fileset

        Returns:
            str: JSON-formatted string containing full fileset metadata
        """
        pass

    # pylint: disable=too-many-positional-arguments
    @abstractmethod
    async def list_files_in_fileset(
        self,
        catalog_name: str,
        schema_name: str,
        fileset_name: str,
        location_name: str,
        sub_path: str = "/",
    ) -> str:
        """
        List files in a specific fileset.

        Args:
            catalog_name: Name of the catalog
            schema_name: Name of the schema
            fileset_name: Name of the fileset
            sub_path: Sub-path within the fileset to list files from (default is root "/")
            location_name: Name of the location

        Returns:
            str: JSON-formatted string containing list of files in the fileset
        """
        pass
