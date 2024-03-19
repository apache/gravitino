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

    @classmethod
    def of(cls, *names) -> Type['NameIdentifier']:
        """
        Create a NameIdentifier with the given levels of names.

        Args:
            names(str): The names of the identifier.
        """
        assert len(names) > 0
        return cls(Namespace(names[:-1]), names[-1])

    @classmethod
    def parse(cls, identity:str) ->  Type['NameIdentifier']:
        """
        """
        assert identity is not None
        assert len(identity) > 0
        return cls(tuple(identity.replace(' ', '').split(',')))

    def hasNamespace(self) -> bool:
        return not self.__namespace.is_empty()

    def namespace(self) -> Namespace:
        return self.__namespace
    
    def name(self) -> str:
        return self.__name

    def __eq__(self, __value: object) -> bool:
        if id(self) == id(__value):
            return True

        if isinstance(__value, NameIdentifier):
            return self.__namespace == __value.__namespace and self.__name == __value.__name
        else:
            return False
