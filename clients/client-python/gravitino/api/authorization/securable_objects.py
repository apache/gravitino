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
from __future__ import annotations

from abc import ABC, abstractmethod
from collections import Counter
from collections.abc import Collection

from gravitino.api.authorization.privileges import Privilege
from gravitino.api.metadata_object import MetadataObject
from gravitino.api.metadata_objects import MetadataObjects


class SecurableObject(MetadataObject, ABC):
    """
    A securable object is an entity on which access control can be granted.
    Unless explicitly granted, access is denied.

    Apache Gravitino organizes securable objects in a tree structure.
    Each securable object contains three attributes: parent, name, and type.

    Supported types:
        - CATALOG
        - SCHEMA
        - TABLE
        - FILESET
        - TOPIC
        - METALAKE

    Use the helper class `SecurableObjects` to construct the securable object you need.

    In RESTful APIs, you can reference a securable object using its full name and type.

    Examples
    --------
    Catalog:
        - Python code:
            SecurableObjects.catalog("catalog1")
        - REST API:
            full_name="catalog1", type="CATALOG"

    Schema:
        - Python code:
            SecurableObjects.schema("catalog1", "schema1")
        - REST API:
            full_name="catalog1.schema1", type="SCHEMA"

    Table:
        - Python code:
            SecurableObjects.table("catalog1", "schema1", "table1")
        - REST API:
            full_name="catalog1.schema1.table1", type="TABLE"

    Topic:
        - Python code:
            SecurableObjects.topic("catalog1", "schema1", "topic1")
        - REST API:
            full_name="catalog1.schema1.topic1", type="TOPIC"

    Fileset:
        - Python code:
            SecurableObjects.fileset("catalog1", "schema1", "fileset1")
        - REST API:
            full_name="catalog1.schema1.fileset1", type="FILESET"

    Metalake:
        - Python code:
            SecurableObjects.metalake("metalake1")
        - REST API:
            full_name="metalake1", type="METALAKE"

    Notes
    -----
    - To represent “all catalogs”, you can use the metalake as the root object.
    - To grant a privilege on all children, you can assign it to their common parent.
      For example, to grant READ TABLE on all tables under `catalog1.schema1`,
      simply grant READ TABLE on the schema object itself.
    """

    @abstractmethod
    def privileges(self) -> list["Privilege"]:
        """The privileges of the securable object. For example: If the securable object is a table, the
        privileges could be `READ TABLE`, `WRITE TABLE`, etc. If a schema has the privilege of `LOAD
        TABLE`. It means the role can load all tables of the schema.

        returns:
            The privileges of the securable object.
        """
        raise NotImplementedError()


class SecurableObjects:
    """The helper class for SecurableObject."""

    class SecurableObjectImpl(MetadataObjects.MetadataObjectImpl, SecurableObject):
        def __init__(
            self,
            parent: str,
            name: str,
            type_: MetadataObject.Type,
            privileges: list[Privilege],
        ) -> None:
            super().__init__(name, type_, parent)
            self._privileges = list(set(privileges))

        @staticmethod
        def is_equal_collection(c1: Collection, c2: Collection) -> bool:
            if c1 is c2:
                return True
            if c1 is None or c2 is None:
                return False

            return Counter(c1) == Counter(c2)

        def __hash__(self) -> int:
            return hash((super().__hash__(), self._privileges))

        def __str__(self) -> str:
            privileges_str = ",".join(
                f"[{p.simple_string()}]" for p in self._privileges
            )

            return (
                f"SecurableObject: [fullName={self.full_name()}], "
                f"[type={self.type()}], [privileges={privileges_str}]"
            )

        def __eq__(self, other: object) -> bool:
            if other is self:
                return True
            if not isinstance(other, SecurableObject):
                return False

            return super().__eq__(
                other
            ) and SecurableObjects.SecurableObjectImpl.is_equal_collection(
                self.privileges(), other.privileges()
            )

        def privileges(self) -> list[Privilege]:
            return self._privileges

    @staticmethod
    def of_metalake(
        metalake: str,
        privileges: list[Privilege],
    ) -> SecurableObjectImpl:
        """
        Create the metalake SecurableObject with the given metalake name and privileges.

        Args:
            metalake (str): The metalake name.
            privileges (list[Privilege]): The privileges of the metalake.

        Returns:
            SecurableObjectImpl: The created metalake SecurableObject.
        """
        return SecurableObjects.of(
            MetadataObject.Type.METALAKE,
            [metalake],
            privileges,
        )

    @staticmethod
    def of_catalog(
        catalog: str,
        privileges: list[Privilege],
    ) -> SecurableObjectImpl:
        """
        Create the catalog SecurableObject with the given catalog name and privileges.

        Args:
            catalog (str): The catalog name
            privileges (list[Privilege]): The privileges of the catalog.

        Returns:
            SecurableObjectImpl: The created catalog SecurableObject.
        """
        return SecurableObjects.of(
            MetadataObject.Type.CATALOG,
            [catalog],
            privileges,
        )

    @staticmethod
    def of_schema(
        catalog: SecurableObject,
        schema: str,
        privileges: list[Privilege],
    ) -> SecurableObjectImpl:
        """
        Create the schema SecurableObject with the given securable catalog object, schema name and privileges.

        Args:
            catalog (SecurableObject): The catalog securable object.
            schema (str): The schema name.
            privileges (list[Privilege]): The privileges of the schema.

        Returns:
            SecurableObjectImpl: The created schema SecurableObject.
        """
        return SecurableObjects.of(
            MetadataObject.Type.SCHEMA,
            [catalog.full_name(), schema],
            privileges,
        )

    @staticmethod
    def of_table(
        schema: SecurableObject,
        table: str,
        privileges: list[Privilege],
    ) -> SecurableObjectImpl:
        """
        Create the table SecurableObject with the given securable schema object, table name and privileges.

        Args:
            schema (SecurableObject): The schema securable object.
            table (str): The table name.
            privileges (list[Privilege]): The privileges of the table.

        Returns:
            SecurableObjectImpl: The created table SecurableObject.
        """
        names = [*schema.full_name().split("."), table]
        return SecurableObjects.of(
            MetadataObject.Type.TABLE,
            names,
            privileges,
        )

    @staticmethod
    def of_topic(
        schema: SecurableObject,
        topic: str,
        privileges: list[Privilege],
    ) -> SecurableObjectImpl:
        """
        Create the topic SecurableObject with the given securable schema object ,topic name and privileges.

        Args:
            schema (SecurableObject): The schema securable object
            topic (str): The topic name
            privileges (list[Privilege]): The privileges of the topic

        Returns:
            SecurableObjectImpl: The created topic SecurableObject
        """
        names = schema.full_name().split(".")
        names.append(topic)
        return SecurableObjects.of(
            MetadataObject.Type.TOPIC,
            names,
            privileges,
        )

    @staticmethod
    def of_fileset(
        schema: SecurableObject,
        fileset: str,
        privileges: list[Privilege],
    ) -> SecurableObjectImpl:
        """
        Create the model SecurableObject with the given securable schema object, fileset name and

        Args:
            schema (SecurableObject): The schema securable object
            fileset (str): The fileset name
            privileges (list[Privilege]): The privileges of the fileset

        Returns:
            SecurableObjectImpl: The created fileset SecurableObject.
        """
        names = schema.full_name().split(".")
        names.append(fileset)
        return SecurableObjects.of(
            MetadataObject.Type.FILESET,
            names,
            privileges,
        )

    @staticmethod
    def of_model(
        schema: SecurableObject,
        model: str,
        privileges: list[Privilege],
    ) -> SecurableObjectImpl:
        """
        Create the model SecurableObject with the given securable schema object, model name and

        Args:
            schema (SecurableObject): The schema securable object.
            model (str): The model name.
            privileges (list[Privilege]): The privileges of the fileset.

        Returns:
            SecurableObjectImpl: The created model SecurableObject.
        """
        names = schema.full_name().split(".")
        names.append(model)
        return SecurableObjects.of(
            MetadataObject.Type.MODEL,
            names,
            privileges,
        )

    @staticmethod
    def of_tag(
        tag_name: str,
        privileges: list[Privilege],
    ) -> SecurableObjectImpl:
        """
        Create the tag SecurableObject with the given tag name and privileges.

        Args:
            tag_name (str): The tag name
            privileges (list[Privilege]): The privileges of the tag

        Returns:
            SecurableObjectImpl: The created tag SecurableObject.
        """
        return SecurableObjects.of(
            MetadataObject.Type.TAG,
            [tag_name],
            privileges,
        )

    @staticmethod
    def of_policy(
        policy: str,
        privileges: list[Privilege],
    ) -> SecurableObjectImpl:
        """
        Create the policy SecurableObject with the given policy name and privileges.

        Args:
            policy (str): The policy name
            privileges (list[Privilege]): The privileges of the policy

        Returns:
            SecurableObjectImpl: _descriThe created policy SecurableObject.
        """
        return SecurableObjects.of(
            MetadataObject.Type.POLICY,
            [policy],
            privileges,
        )

    @staticmethod
    def of_job_template(
        job_template: str,
        privileges: list[Privilege],
    ) -> SecurableObjectImpl:
        """
        Create the job template SecurableObject with the given job template name and privileges.

        Args:
            job_template (str): The job template name
            privileges (list[Privilege]): The privileges of the job template

        Returns:
            SecurableObjectImpl: The created job template SecurableObject.
        """
        return SecurableObjects.of(
            MetadataObject.Type.JOB_TEMPLATE,
            [job_template],
            privileges,
        )

    @staticmethod
    def parse(
        full_name: str,
        type_: MetadataObject.Type,
        privileges: list[Privilege],
    ) -> SecurableObject:
        """
        Create a SecurableObject from the given full name.

        Args:
            full_name (str): The full name of securable object.
            type_ (MetadataObject.Type): The securable object type.
            privileges (list[Privilege]): The securable object privileges.

        Returns:
            SecurableObject: The created SecurableObject.
        """
        metadata_object = MetadataObjects.parse(full_name, type_)
        return SecurableObjects.SecurableObjectImpl(
            metadata_object.parent(),
            metadata_object.name(),
            type_,
            privileges,
        )

    @staticmethod
    def of(
        type_: MetadataObject.Type,
        names: list[str],
        privileges: list[Privilege],
    ) -> SecurableObject:
        """
        Create the SecurableObject with the given names.

        Args:
            type_ (MetadataObject.Type): The securable object type.
            names (list[str]): The names of the securable object.
            privileges (list[Privilege]): The securable object privileges.

        Returns:
            SecurableObject: he created SecurableObject
        """
        metadata_object = MetadataObjects.of(names, type_)
        return SecurableObjects.SecurableObjectImpl(
            metadata_object.parent(),
            metadata_object.name(),
            type_,
            privileges,
        )
