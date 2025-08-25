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


class ModelOperation(ABC):
    """
    Abstract base class for Gravitino model operations.
    """

    @abstractmethod
    async def list_of_models(self, catalog_name: str, schema_name: str) -> str:
        """
        Retrieve the list of models within a specified catalog.

        Args:
            catalog_name: Name of the catalog
            schema_name: Name of the schema

        Returns:
            str: JSON-formatted string containing model list information
        """
        pass

    @abstractmethod
    async def load_model(
        self, catalog_name: str, schema_name: str, model_name: str
    ) -> str:
        """
        Load detailed information of a specific model.

        Args:
            catalog_name: Name of the catalog
            schema_name: Name of the schema
            model_name: Name of the model

        Returns:
            str: JSON-formatted string containing full model metadata
        """
        pass

    @abstractmethod
    async def list_model_versions(
        self, catalog_name: str, schema_name: str, model_name: str
    ) -> str:
        """
        List all versions of a specific model.

        Args:
            catalog_name: Name of the catalog
            schema_name: Name of the schema
            model_name: Name of the model

        Returns:
            str: JSON-formatted string containing model version information
        """
        pass

    @abstractmethod
    async def load_model_version(
        self, catalog_name: str, schema_name: str, model_name: str, version: int
    ) -> str:
        """
        Load detailed information of a specific version of a model.

        Args:
            catalog_name: Name of the catalog
            schema_name: Name of the schema
            model_name: Name of the model
            version: Version identifier of the model

        Returns:
            str: JSON-formatted string containing full model version metadata
        """
        pass

    @abstractmethod
    async def load_model_version_by_alias(
        self, catalog_name: str, schema_name: str, model_name: str, alias: str
    ) -> str:
        """
        Load detailed information of a specific version of a model by its alias.

        Args:
            catalog_name: Name of the catalog
            schema_name: Name of the schema
            model_name: Name of the model
            alias: Alias identifier of the model version

        Returns:
            str: JSON-formatted string containing full model version metadata
        """
        pass
