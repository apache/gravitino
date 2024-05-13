"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

from typing import ClassVar

from dataclasses import dataclass, field

from dataclasses_json import DataClassJsonMixin, config

from gravitino.exceptions.illegal_name_identifier_exception import (
    IllegalNameIdentifierException,
)
from gravitino.namespace import Namespace


@dataclass
class NameIdentifier(DataClassJsonMixin):
    """A name identifier is a sequence of names separated by dots. It's used to identify a metalake, a
    catalog, a schema or a table. For example, "metalake1" can represent a metalake,
    "metalake1.catalog1" can represent a catalog, "metalake1.catalog1.schema1" can represent a
    schema.
    """

    _DOT: ClassVar[str] = "."

    _name: str = field(metadata=config(field_name="name"))
    _namespace: Namespace = field(metadata=config(field_name="namespace"))

    @classmethod
    def builder(cls, namespace: Namespace, name: str):
        return NameIdentifier(_namespace=namespace, _name=name)

    def namespace(self):
        return self._namespace

    def name(self):
        return self._name

    @staticmethod
    def of(*names: str) -> "NameIdentifier":
        """Create the NameIdentifier with the given levels of names.

        Args:
            names The names of the identifier

        Returns:
            The created NameIdentifier
        """

        NameIdentifier.check(
            names is not None, "Cannot create a NameIdentifier with null names"
        )
        NameIdentifier.check(
            len(names) > 0, "Cannot create a NameIdentifier with no names"
        )

        return NameIdentifier.builder(Namespace.of(*names[:-1]), names[-1])

    @staticmethod
    def of_namespace(namespace: Namespace, name: str) -> "NameIdentifier":
        """Create the NameIdentifier with the given Namespace and name.

        Args:
            namespace: The namespace of the identifier
            name: The name of the identifier

        Returns:
            The created NameIdentifier
        """
        return NameIdentifier.builder(namespace, name)

    @staticmethod
    def of_metalake(metalake: str) -> "NameIdentifier":
        """Create the metalake NameIdentifier with the given name.

        Args:
            metalake: The metalake name

        Returns:
            The created metalake NameIdentifier
        """
        return NameIdentifier.of(metalake)

    @staticmethod
    def of_catalog(metalake: str, catalog: str) -> "NameIdentifier":
        """Create the catalog NameIdentifier with the given metalake and catalog name.

        Args:
            metalake: The metalake name
            catalog: The catalog name

        Returns:
            The created catalog NameIdentifier
        """
        return NameIdentifier.of(metalake, catalog)

    @staticmethod
    def of_schema(metalake: str, catalog: str, schema: str) -> "NameIdentifier":
        """Create the schema NameIdentifier with the given metalake, catalog and schema name.

        Args:
            metalake: The metalake name
            catalog: The catalog name
            schema: The schema name

        Returns:
            The created schema NameIdentifier
        """
        return NameIdentifier.of(metalake, catalog, schema)

    @staticmethod
    def of_table(
        metalake: str, catalog: str, schema: str, table: str
    ) -> "NameIdentifier":
        """Create the table NameIdentifier with the given metalake, catalog, schema and table name.

        Args:
            metalake: The metalake name
            catalog: The catalog name
            schema: The schema name
            table: The table name

        Returns:
            The created table NameIdentifier
        """
        return NameIdentifier.of(metalake, catalog, schema, table)

    @staticmethod
    def of_fileset(
        metalake: str, catalog: str, schema: str, fileset: str
    ) -> "NameIdentifier":
        """Create the fileset NameIdentifier with the given metalake, catalog, schema and fileset name.

        Args:
            metalake: The metalake name
            catalog: The catalog name
            schema: The schema name
            fileset: The fileset name

        Returns:
            The created fileset NameIdentifier
        """
        return NameIdentifier.of(metalake, catalog, schema, fileset)

    @staticmethod
    def of_topic(
        metalake: str, catalog: str, schema: str, topic: str
    ) -> "NameIdentifier":
        """Create the topic NameIdentifier with the given metalake, catalog, schema and topic
        name.

        Args:
            metalake: The metalake name
            catalog: The catalog name
            schema: The schema name
            topic: The topic name

        Returns:
            The created topic NameIdentifier
        """
        return NameIdentifier.of(metalake, catalog, schema, topic)

    @staticmethod
    def check_metalake(ident: "NameIdentifier") -> None:
        """Check the given NameIdentifier is a metalake identifier. Throw an {@link
        IllegalNameIdentifierException} if it's not.

        Args:
            ident: The metalake NameIdentifier to check.
        """
        NameIdentifier.check(ident is not None, "Metalake identifier must not be null")
        Namespace.check_metalake(ident.namespace())

    @staticmethod
    def check_catalog(ident: "NameIdentifier") -> None:
        """Check the given NameIdentifier is a catalog identifier. Throw an {@link
        IllegalNameIdentifierException} if it's not.

        Args:
            ident: The catalog NameIdentifier to check.
        """
        NameIdentifier.check(ident is not None, "Catalog identifier must not be null")
        Namespace.check_catalog(ident.namespace())

    @staticmethod
    def check_schema(ident: "NameIdentifier") -> None:
        """Check the given NameIdentifier is a schema identifier. Throw an {@link
        IllegalNameIdentifierException} if it's not.

        Args:
            ident: The schema NameIdentifier to check.
        """
        NameIdentifier.check(ident is not None, "Schema identifier must not be null")
        Namespace.check_schema(ident.namespace())

    @staticmethod
    def check_table(ident: "NameIdentifier") -> None:
        """Check the given NameIdentifier is a table identifier. Throw an {@link
        IllegalNameIdentifierException} if it's not.

        Args:
            ident: The table NameIdentifier to check.
        """
        NameIdentifier.check(ident is not None, "Table identifier must not be null")
        Namespace.check_table(ident.namespace())

    @staticmethod
    def check_fileset(ident: "NameIdentifier") -> None:
        """Check the given NameIdentifier is a fileset identifier. Throw an {@link
        IllegalNameIdentifierException} if it's not.

        Args:
            ident: The fileset NameIdentifier to check.
        """
        NameIdentifier.check(ident is not None, "Fileset identifier must not be null")
        Namespace.check_fileset(ident.namespace())

    @staticmethod
    def check_topic(ident: "NameIdentifier") -> None:
        """Check the given NameIdentifier is a topic identifier. Throw an {@link
        IllegalNameIdentifierException} if it's not.

        Args:
            ident: The topic NameIdentifier to check.
        """
        NameIdentifier.check(ident is not None, "Topic identifier must not be null")
        Namespace.check_topic(ident.namespace())

    @staticmethod
    def parse(identifier: str) -> "NameIdentifier":
        """Create a NameIdentifier from the given identifier string.

        Args:
            identifier: The identifier string

        Returns:
            The created NameIdentifier
        """
        NameIdentifier.check(
            identifier is not None and identifier != "",
            "Cannot parse a null or empty identifier",
        )

        parts = identifier.split(NameIdentifier._DOT)
        return NameIdentifier.of(*parts)

    def has_namespace(self):
        """Check if the NameIdentifier has a namespace.

        Returns:
            True if the NameIdentifier has a namespace, false otherwise.
        """
        return not self.namespace().is_empty()

    def get_namespace(self):
        """Get the namespace of the NameIdentifier.

        Returns:
            The namespace of the NameIdentifier.
        """
        return self._namespace

    def get_name(self):
        """Get the name of the NameIdentifier.

        Returns:
            The name of the NameIdentifier.
        """
        return self._name

    def __eq__(self, other):
        if not isinstance(other, NameIdentifier):
            return False
        return self._namespace == other._namespace and self._name == other._name

    def __hash__(self):
        return hash(self._namespace, self._name)

    def __str__(self):
        if self.has_namespace():
            return str(self._namespace) + "." + self._name
        return self._name

    @staticmethod
    def check(condition, message, *args):
        """Check the given condition is true. Throw an {@link IllegalNameIdentifierException} if it's not.

        Args:
            condition: The condition to check.
            message: The message to throw.
            args: The arguments to the message.
        """
        if not condition:
            raise IllegalNameIdentifierException(message.format(*args))
