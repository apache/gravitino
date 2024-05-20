"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

from abc import abstractmethod
from dataclasses import dataclass, field

from dataclasses_json import config

from gravitino.api.schema_change import SchemaChange
from gravitino.rest.rest_message import RESTRequest


@dataclass
class SchemaUpdateRequestBase(RESTRequest):
    _type: str = field(metadata=config(field_name="@type"))

    def __init__(self, type: str):
        self._type = type

    @abstractmethod
    def schema_change(self):
        pass


@dataclass
class SchemaUpdateRequest:
    """Represents an interface for Schema update requests."""

    @dataclass
    class SetSchemaPropertyRequest(SchemaUpdateRequestBase):
        """Represents a request to set a property on a Schema."""

        _property: str = field(metadata=config(field_name="property"))
        """The property to set."""

        _value: str = field(metadata=config(field_name="value"))
        """The value of the property."""

        def __init__(self, property: str, value: str):
            super().__init__("setProperty")
            self._property = property
            self._value = value

        def validate(self):
            """Validates the fields of the request.

            Raises:
                 IllegalArgumentException if property or value are not set.
            """
            if not self._property:
                raise ValueError('"property" field is required and cannot be empty')
            if not self._value:
                raise ValueError('"value" field is required and cannot be empty')

        def schema_change(self):
            return SchemaChange.set_property(self._property, self._value)

    @dataclass
    class RemoveSchemaPropertyRequest(SchemaUpdateRequestBase):
        """Represents a request to remove a property from a Schema."""

        _property: str = field(metadata=config(field_name="property"))
        """The property to remove."""

        def __init__(self, property: str):
            super().__init__("removeProperty")
            self._property = property

        def validate(self):
            """Validates the fields of the request.

            Raises:
                 IllegalArgumentException if property is not set.
            """
            if not self._property:
                raise ValueError('"property" field is required and cannot be empty')

        def schema_change(self):
            return SchemaChange.remove_property(self._property)
