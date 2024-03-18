"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""
from .namespace import Namespace
from typing import Type

class NameIdentifier:
    """
    A name identifier is a sequence of names separated by dots. It's used to identify a metalake, a
    catalog, a schema or a table. For example, "metalake1" can represent a metalake,
    "metalake1.catalog1" can represent a catalog, "metalake1.catalog1.schema1" can represent a
    schema.
    """
    __namespace: Namespace
    __name: str

    def __init__(self, namespace, name):
        """
        Initializes a new instance of the NameIdentifier.

        Args:
            names(str): The names of the identifier.
        """
        self.__namespace = namespace
        self.__name = name

    def of(self, *names) -> Type['NameIdentifier']:
        """
        Create a NameIdentifier with the given levels of names.

        Args:
            names(str): The names of the identifier.
        """
        assert len(names) > 0
        return NameIdentifier(Namespace(names[:-1]), names[-1])


    def of(self, namespace, name) ->Type['NameIdentifier']:
        """
        Create a NameIdentifier with the namespace and the name.
        Args:
            namespace (Namespace): the namespace of the NameIdentifier.
            name (str): the name of the NameIdentifier.
        Return:
            NameIdentifier: the created NameIdentofier.
        """
        return NameIdentifier(namespace, name)



