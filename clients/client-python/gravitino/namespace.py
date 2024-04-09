"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""
from typing import List


class Namespace:
    """A namespace is a sequence of levels separated by dots. It's used to identify a metalake, a
    catalog or a schema. For example, "metalake1", "metalake1.catalog1" and
    "metalake1.catalog1.schema1" are all valid namespaces.
    """

    EMPTY = None
    DOT = "."

    def __init__(self, levels: List[str]):
        self.levels = levels

    @staticmethod
    def empty() -> 'Namespace':
        """Get an empty namespace.

        Returns:
            An empty namespace
        """
        return Namespace([])

    @staticmethod
    def of(*levels: str) -> 'Namespace':
        """Create a namespace with the given levels.

        Args:
            levels The levels of the namespace

        Returns:
            A namespace with the given levels
        """
        if levels is None:
            raise ValueError("Cannot create a namespace with null levels")
        if len(levels) == 0:
            return Namespace.empty()

        for level in levels:
            if level is None or level == "":
                raise ValueError("Cannot create a namespace with null or empty level")

        return Namespace(list(levels))

    @staticmethod
    def of_metalake() -> 'Namespace':
        """Create a namespace for metalake.

        Returns:
            A namespace for metalake
        """
        return Namespace.empty()

    @staticmethod
    def of_catalog(metalake: str) -> 'Namespace':
        """Create a namespace for catalog.

        Args:
            metalake: The metalake name

        Returns:
            A namespace for catalog
        """
        return Namespace.of(metalake)

    @staticmethod
    def of_schema(metalake: str, catalog: str) -> 'Namespace':
        """Create a namespace for schema.

        Args:
            metalake: The metalake name
            catalog: The catalog name

        Returns:
             A namespace for schema
        """
        return Namespace.of(metalake, catalog)

    @staticmethod
    def of_table(metalake: str, catalog: str, schema: str) -> 'Namespace':
        """Create a namespace for table.

        Args:
            metalake: The metalake name
            catalog: The catalog name
            schema: The schema name

        Return:
             A namespace for table
        """
        return Namespace.of(metalake, catalog, schema)

    @staticmethod
    def of_fileset(metalake: str, catalog: str, schema: str) -> 'Namespace':
        """Create a namespace for fileset.

        Args:
            metalake: The metalake name
            catalog: The catalog name
            schema: The schema name

        Return:
             A namespace for fileset
        """
        return Namespace.of(metalake, catalog, schema)

    @staticmethod
    def of_topic(metalake: str, catalog: str, schema: str) -> 'Namespace':
        """Create a namespace for topic.

        Args:
            metalake: The metalake name
            catalog: The catalog name
            schema: The schema name

        Return:
             A namespace for topic
        """
        return Namespace.of(metalake, catalog, schema)

    @staticmethod
    def check_metalake(namespace: 'Namespace') -> None:
        """Check if the given metalake namespace is legal, throw an IllegalNamespaceException if
        it's illegal.

        Args:
            namespace: The metalake namespace
        """
        if not namespace and not namespace.is_empty():
            raise ValueError(f"Metalake namespace must be non-null and empty, the input namespace is {namespace}")

    @staticmethod
    def check_catalog(namespace: 'Namespace') -> None:
        """Check if the given catalog namespace is legal, throw an IllegalNamespaceException if
        it's illegal.

        Args:
            namespace: The catalog namespace
        """
        if not namespace and namespace.length() != 1:
            raise ValueError(f"Catalog namespace must be non-null and have 1 level, the input namespace is {namespace}")

    @staticmethod
    def check_schema(namespace: 'Namespace') -> None:
        """Check if the given schema namespace is legal, throw an IllegalNamespaceException if
        it's illegal.

        Args:
            namespace: The schema namespace
        """
        if not namespace and namespace.length() != 2:
            raise ValueError(f"Schema namespace must be non-null and have 2 levels, the input namespace is {namespace}")

    @staticmethod
    def check_table(namespace: 'Namespace') -> None:
        """Check if the given table namespace is legal, throw an IllegalNamespaceException if it's
        illegal.

        Args:
            namespace: The table namespace
        """
        if not namespace and namespace.length() != 3:
            raise ValueError(f"Table namespace must be non-null and have 3 levels, the input namespace is {namespace}")

    @staticmethod
    def check_fileset(namespace: 'Namespace') -> None:
        """Check if the given fileset namespace is legal, throw an IllegalNamespaceException if
        it's illegal.

        Args:
            namespace: The fileset namespace
        """
        if not namespace and namespace.length() != 3:
            raise ValueError(
                f"Fileset namespace must be non-null and have 3 levels, the input namespace is {namespace}")

    @staticmethod
    def check_topic(namespace: 'Namespace') -> None:
        """Check if the given topic namespace is legal, throw an IllegalNamespaceException if it's
        illegal.

        Args:
            namespace: The topic namespace
        """
        if not namespace and namespace.length() != 3:
            raise ValueError(f"Topic namespace must be non-null and have 3 levels, the input namespace is {namespace}")

    def levels(self) -> List[str]:
        """Get the levels of the namespace.

        Return:
            The levels of the namespace
        """
        return self.levels

    def level(self, pos: int) -> str:
        """Get the level at the given position.

        Args:
            pos: The position of the level

        Return:
            The level at the given position
        """
        if pos < 0 or pos >= len(self.levels):
            raise ValueError("Invalid level position")
        return self.levels[pos]

    def length(self) -> int:
        """Get the length of the namespace.

        Return:
            The length of the namespace.
        """
        return len(self.levels)

    def is_empty(self) -> bool:
        """Check if the namespace is empty.

        Return:
            True if the namespace is empty, false otherwise.
        """
        return len(self.levels) == 0

    def __eq__(self, other: 'Namespace') -> bool:
        if not isinstance(other, Namespace):
            return False
        return self.levels == other.levels

    def __hash__(self) -> int:
        return hash(tuple(self.levels))

    def __str__(self) -> str:
        return Namespace.DOT.join(self.levels)

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
