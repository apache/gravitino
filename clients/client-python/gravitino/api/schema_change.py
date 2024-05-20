"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

from abc import ABC
from dataclasses import field

from dataclasses_json import config


class SchemaChange(ABC):
    """NamespaceChange class to set the property and value pairs for the namespace."""

    @staticmethod
    def set_property(property: str, value: str):
        """SchemaChange class to set the property and value pairs for the schema.

        Args:
            property: The property name to set.
            value: The value to set the property to.

        Returns:
             The SchemaChange object.
        """
        return SchemaChange.SetProperty(property, value)

    @staticmethod
    def remove_property(property: str):
        """SchemaChange class to remove a property from the schema.

        Args:
            property: The property name to remove.

        Returns:
            The SchemaChange object.
        """
        return SchemaChange.RemoveProperty(property)

    class SetProperty:
        """SchemaChange class to set the property and value pairs for the schema."""

        _property: str = field(metadata=config(field_name="property"))
        _value: str = field(metadata=config(field_name="value"))

        def __init__(self, property: str, value: str):
            self._property = property
            self._value = value

        def property(self):
            """Retrieves the name of the property to be set.

            Returns:
                 The name of the property.
            """
            return self._property

        def value(self):
            """Retrieves the value of the property to be set.

            Returns:
                 The value of the property.
            """
            return self._value

        def __eq__(self, other):
            """Compares this SetProperty instance with another object for equality.
            Two instances are considered equal if they have the same property and value.

            Args:
                 other: The object to compare with this instance.

            Returns:
                 true if the given object represents the same property setting; false otherwise.
            """
            if not isinstance(other, SchemaChange.SetProperty):
                return False
            return self._property == other.property() and self._value == other.value()

        def __hash__(self):
            """Generates a hash code for this SetProperty instance.
            The hash code is based on both the property name and its value.

             Returns:
                  A hash code value for this property setting.
            """
            return hash((self._property, self._value))

        def __str__(self):
            """Provides a string representation of the SetProperty instance.
            This string format includes the class name followed by the property name and its value.

            Returns:
                 A string summary of the property setting.
            """
            return f"SETPROPERTY {self._property} {self._value}"

    class RemoveProperty:
        """SchemaChange class to remove a property from the schema."""

        _property: str = field(metadata=config(field_name="property"))

        def __init__(self, property: str):
            self._property = property

        def property(self):
            """Retrieves the name of the property to be removed.

            Returns:
                 The name of the property for removal.
            """
            return self._property

        def __eq__(self, other):
            """Compares this RemoveProperty instance with another object for equality.
            Two instances are considered equal if they target the same property for removal.

            Args:
                other: The object to compare with this instance.

            Returns:
                 true if the given object represents the same property removal; false otherwise.
            """
            if not isinstance(other, SchemaChange.RemoveProperty):
                return False
            return self._property == other.property()

        def __hash__(self):
            """Generates a hash code for this RemoveProperty instance.
            This hash code is based on the property name that is to be removed.

            Returns:
                 A hash code value for this property removal operation.
            """
            return hash(self._property)

        def __str__(self):
            """Provides a string representation of the RemoveProperty instance.
            This string format includes the class name followed by the property name to be removed.

            Returns:
                 A string summary of the property removal operation.
            """
            return f"REMOVEPROPERTY {self._property}"
