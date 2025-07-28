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

from abc import abstractmethod
from enum import Enum
from typing import Dict, Optional

from gravitino.api.auditable import Auditable
from gravitino.api.supports_schemas import SupportsSchemas


class Catalog(Auditable):
    """The interface of a catalog. The catalog is the second level entity in the gravitino system,
    containing a set of tables.
    """

    class Type(Enum):
        """The type of the catalog."""

        RELATIONAL = ("relational", False)
        """"Catalog Type for Relational Data Structure, like db.table, catalog.db.table."""

        FILESET = ("fileset", True)
        """Catalog Type for Fileset System (including HDFS, S3, etc.), like path/to/file"""

        MESSAGING = ("messaging", False)
        """Catalog Type for Message Queue, like kafka://topic"""

        MODEL = ("model", True)
        """Catalog Type for ML model"""

        UNSUPPORTED = ("unsupported", False)
        """Catalog Type for test only."""

        def __init__(self, type_name, supports_managed_catalog):
            self._type_name = type_name
            self._supports_managed_catalog = supports_managed_catalog

        @classmethod
        def type_serialize(cls, catalog_type):
            return catalog_type.type_name

        @classmethod
        def type_deserialize(cls, type_name):
            for member in cls:
                if member.type_name == type_name:
                    return member
            return cls.UNSUPPORTED

        @property
        def supports_managed_catalog(self):
            """
            A flag to indicate if the catalog type supports managed catalog. Managed catalog is a
            concept in Gravitino, which means Gravitino will manage the lifecycle of the catalog
            and its subsidiaries. If the catalog type supports managed catalog, users can create
            managed catalog of this type without specifying the catalog provider, Gravitino will
            use the type as the provider to create the managed catalog. If the catalog type does
            not support managed catalog, users need to specify the provider to create the catalog.
            """
            return self._supports_managed_catalog

        @property
        def type_name(self):
            """
            The name of the catalog type.
            """
            return self._type_name

    PROPERTY_PACKAGE = "package"
    """A reserved property to specify the package location of the catalog. The "package" is a string
    of path to the folder where all the catalog related dependencies is located. The dependencies
    under the "package" will be loaded by Gravitino to create the catalog.
    
    The property "package" is not needed if the catalog is a built-in one, Gravitino will search
    the proper location using "provider" to load the dependencies. Only when the folder is in
    different location, the "package" property is needed.
    """

    @abstractmethod
    def name(self) -> str:
        """
        Returns:
            The name of the catalog.
        """
        pass

    @abstractmethod
    def type(self) -> Type:
        """
        Returns:
            The type of the catalog.
        """
        pass

    @abstractmethod
    def provider(self) -> str:
        """
        Returns:
            The provider of the catalog.
        """
        pass

    @abstractmethod
    def comment(self) -> Optional[str]:
        """The comment of the catalog. Note. this method will return null if the comment is not set for
        this catalog.

        Returns:
            The provider of the catalog.
        """
        pass

    @abstractmethod
    def properties(self) -> Optional[Dict[str, str]]:
        """
        The properties of the catalog. Note, this method will return null if the properties are not set.

        Returns:
            The properties of the catalog.
        """
        pass

    def as_schemas(self) -> SupportsSchemas:
        """Return the {@link SupportsSchemas} if the catalog supports schema operations.

        Raises:
            UnsupportedOperationException if the catalog does not support schema operations.

        Returns:
            The {@link SupportsSchemas} if the catalog supports schema operations.
        """
        raise UnsupportedOperationException(
            "Catalog does not support schema operations"
        )

    def as_table_catalog(self) -> "TableCatalog":  # noqa: F821
        """
        Raises:
            UnsupportedOperationException if the catalog does not support table operations.

        Returns:
            the {@link TableCatalog} if the catalog supports table operations.
        """
        raise UnsupportedOperationException("Catalog does not support table operations")

    def as_fileset_catalog(self) -> "FilesetCatalog":  # noqa: F821
        """
        Raises:
            UnsupportedOperationException if the catalog does not support fileset operations.

        Returns:
            the FilesetCatalog if the catalog supports fileset operations.
        """
        raise UnsupportedOperationException(
            "Catalog does not support fileset operations"
        )

    def as_topic_catalog(self) -> "TopicCatalog":  # noqa: F821
        """
        Returns:
            the {@link TopicCatalog} if the catalog supports topic operations.

        Raises:
            UnsupportedOperationException if the catalog does not support topic operations.
        """
        raise UnsupportedOperationException("Catalog does not support topic operations")

    def as_model_catalog(self) -> "ModelCatalog":  # noqa: F821
        """
        Returns:
            the {@link ModelCatalog} if the catalog supports model operations.

        Raises:
            UnsupportedOperationException if the catalog does not support model operations.
        """
        raise UnsupportedOperationException("Catalog does not support model operations")


class UnsupportedOperationException(Exception):
    pass
