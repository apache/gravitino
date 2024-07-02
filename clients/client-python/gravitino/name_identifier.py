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

# TODO: delete redundant methods


@dataclass
class NameIdentifier(DataClassJsonMixin):
    """A name identifier is a sequence of names separated by dots. It's used to identify a metalake, a
    catalog, a schema or a table. For example, "metalake1" can represent a metalake,
    "metalake1.catalog1" can represent a catalog, "metalake1.catalog1.schema1" can represent a
    schema.
    """

    _DOT: ClassVar[str] = "."

    _name: str = field(metadata=config(field_name="name"))
    _namespace: Namespace = field(
        metadata=config(
            field_name="namespace",
            encoder=Namespace.to_json,
            decoder=Namespace.from_json,
        )
    )

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
        return hash((self._namespace, self._name))

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
