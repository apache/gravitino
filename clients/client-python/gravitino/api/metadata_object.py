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
from enum import Enum


class MetadataObject(ABC):
    """The MetadataObject is the basic unit of the Gravitino system. It
    represents the metadata object in the Apache Gravitino system. The object
    can be a metalake, catalog, schema, table, topic, etc.
    """

    class Type(Enum):
        """The type of object in the Gravitino system. Every type will map one
        kind of the entity of the underlying system."""

        CATALOG = "catalog"
        """"Metadata Type for catalog."""

        FILESET = "fileset"
        """Metadata Type for Fileset System (including HDFS, S3, etc.), like path/to/file"""

    @abstractmethod
    def type(self) -> Type:
        """
        The type of the object.

        Returns:
            The type of the object.
        """
        pass

    @abstractmethod
    def name(self) -> str:
        """
        The name of the object.

        Returns:
            The name of the object.
        """
        pass
