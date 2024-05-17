"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

from typing import List, ClassVar


class Namespace:
    """A namespace is a sequence of levels separated by dots. It's used to identify a metalake, a
    catalog or a schema. For example, "metalake1", "metalake1.catalog1" and
    "metalake1.catalog1.schema1" are all valid namespaces.
    """

    _DOT: ClassVar[str] = "."

    _levels: List[str] = []

    def __init__(self, levels: List[str]):
        self._levels = levels

    @staticmethod
    def empty() -> "Namespace":
        """Get an empty namespace.

        Returns:
            An empty namespace
        """
        return Namespace([])

    @staticmethod
    def of(*levels: str) -> "Namespace":
        """Create a namespace with the given levels.

        Args:
            levels The levels of the namespace

        Returns:
            A namespace with the given levels
        """
        Namespace.check(
            levels is not None, "Cannot create a namespace with null levels"
        )
        if len(levels) == 0:
            return Namespace.empty()

        for level in levels:
            Namespace.check(
                level is not None and level != "",
                "Cannot create a namespace with null or empty level",
            )

        return Namespace(list(levels))

    @staticmethod
    def of_metalake() -> "Namespace":
        """Create a namespace for metalake.

        Returns:
            A namespace for metalake
        """
        return Namespace.empty()

    @staticmethod
    def of_catalog(metalake: str) -> "Namespace":
        """Create a namespace for catalog.

        Args:
            metalake: The metalake name

        Returns:
            A namespace for catalog
        """
        return Namespace.of(metalake)

    @staticmethod
    def of_schema(metalake: str, catalog: str) -> "Namespace":
        """Create a namespace for schema.

        Args:
            metalake: The metalake name
            catalog: The catalog name

        Returns:
             A namespace for schema
        """
        return Namespace.of(metalake, catalog)

    @staticmethod
    def of_table(metalake: str, catalog: str, schema: str) -> "Namespace":
        """Create a namespace for table.

        Args:
            metalake: The metalake name
            catalog: The catalog name
            schema: The schema name

        Returns:
             A namespace for table
        """
        return Namespace.of(metalake, catalog, schema)

    @staticmethod
    def of_fileset(metalake: str, catalog: str, schema: str) -> "Namespace":
        """Create a namespace for fileset.

        Args:
            metalake: The metalake name
            catalog: The catalog name
            schema: The schema name

        Returns:
             A namespace for fileset
        """
        return Namespace.of(metalake, catalog, schema)

    @staticmethod
    def of_topic(metalake: str, catalog: str, schema: str) -> "Namespace":
        """Create a namespace for topic.

        Args:
            metalake: The metalake name
            catalog: The catalog name
            schema: The schema name

        Returns:
             A namespace for topic
        """
        return Namespace.of(metalake, catalog, schema)

    @staticmethod
    def check_metalake(namespace: "Namespace") -> None:
        """Check if the given metalake namespace is legal, throw an IllegalNamespaceException if
        it's illegal.

        Args:
            namespace: The metalake namespace
        """
        Namespace.check(
            namespace is not None and namespace.is_empty(),
            f"Metalake namespace must be non-null and empty, the input namespace is {namespace}",
        )

    @staticmethod
    def check_catalog(namespace: "Namespace") -> None:
        """Check if the given catalog namespace is legal, throw an IllegalNamespaceException if
        it's illegal.

        Args:
            namespace: The catalog namespace
        """
        Namespace.check(
            namespace is not None and namespace.length() == 1,
            f"Catalog namespace must be non-null and have 1 level, the input namespace is {namespace}",
        )

    @staticmethod
    def check_schema(namespace: "Namespace") -> None:
        """Check if the given schema namespace is legal, throw an IllegalNamespaceException if
        it's illegal.

        Args:
            namespace: The schema namespace
        """
        Namespace.check(
            namespace is not None and namespace.length() == 2,
            f"Schema namespace must be non-null and have 2 levels, the input namespace is {namespace}",
        )

    @staticmethod
    def check_table(namespace: "Namespace") -> None:
        """Check if the given table namespace is legal, throw an IllegalNamespaceException if it's
        illegal.

        Args:
            namespace: The table namespace
        """
        Namespace.check(
            namespace is not None and namespace.length() == 3,
            f"Table namespace must be non-null and have 3 levels, the input namespace is {namespace}",
        )

    @staticmethod
    def check_fileset(namespace: "Namespace") -> None:
        """Check if the given fileset namespace is legal, throw an IllegalNamespaceException if
        it's illegal.

        Args:
            namespace: The fileset namespace
        """
        Namespace.check(
            namespace is not None and namespace.length() == 3,
            f"Fileset namespace must be non-null and have 3 levels, the input namespace is {namespace}",
        )

    @staticmethod
    def check_topic(namespace: "Namespace") -> None:
        """Check if the given topic namespace is legal, throw an IllegalNamespaceException if it's
        illegal.

        Args:
            namespace: The topic namespace
        """
        Namespace.check(
            namespace is not None and namespace.length() == 3,
            f"Topic namespace must be non-null and have 3 levels, the input namespace is {namespace}",
        )

    def levels(self) -> List[str]:
        """Get the levels of the namespace.

        Returns:
            The levels of the namespace
        """
        return self._levels

    def level(self, pos: int) -> str:
        """Get the level at the given position.

        Args:
            pos: The position of the level

        Returns:
            The level at the given position
        """
        if pos < 0 or pos >= len(self._levels):
            raise ValueError("Invalid level position")
        return self._levels[pos]

    def length(self) -> int:
        """Get the length of the namespace.

        Returns:
            The length of the namespace.
        """
        return len(self._levels)

    def is_empty(self) -> bool:
        """Check if the namespace is empty.

        Returns:
            True if the namespace is empty, false otherwise.
        """
        return len(self._levels) == 0

    def __eq__(self, other: "Namespace") -> bool:
        if not isinstance(other, Namespace):
            return False
        return self._levels == other._levels

    def __hash__(self) -> int:
        return hash(tuple(self._levels))

    def __str__(self) -> str:
        return self._DOT.join(self._levels)

    @staticmethod
    def check(expression: bool, message: str, *args) -> None:
        """Check the given condition is true. Throw an IllegalNamespaceException if it's not.

        Args:
            expression: The expression to check.
            message: The message to throw.
            args: The arguments to the message.
        """
        if not expression:
            raise ValueError(message.format(*args))
