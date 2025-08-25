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


class TopicOperation(ABC):
    """
    Abstract base class for Gravitino topic operations.
    """

    @abstractmethod
    async def list_of_topics(self, catalog_name: str, schema_name: str) -> str:
        """
        Retrieve the list of topics within a specified catalog.

        Args:
            catalog_name: Name of the catalog
            schema_name: Name of the schema

        Returns:
            str: JSON-formatted string containing topic list information
        """
        pass

    @abstractmethod
    async def load_topic(
        self, catalog_name: str, schema_name: str, topic_name: str
    ) -> str:
        """
        Load detailed information of a specific topic.

        Args:
            catalog_name: Name of the catalog
            schema_name: Name of the schema
            topic_name: Name of the topic

        Returns:
            str: JSON-formatted string containing full topic metadata
        """
        pass
