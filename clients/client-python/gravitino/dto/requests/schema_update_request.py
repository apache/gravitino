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
from dataclasses import dataclass, field
from typing import Dict, Optional

from dataclasses_json import config

from gravitino.api.schema_change import SchemaChange
from gravitino.api.secret import SecretBinding, SecretReference
from gravitino.rest.rest_message import RESTRequest


@dataclass
class SchemaUpdateRequestBase(RESTRequest):
    _type: str = field(metadata=config(field_name="@type"))

    def __init__(self, action_type: str):
        self._type = action_type

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

        def __init__(self, schema_property: str, value: str):
            super().__init__("setProperty")
            self._property = schema_property
            self._value = value

        def validate(self):
            """Validates the fields of the request.

            Raises:
                 IllegalArgumentException if schema_property or value are not set.
            """
            if not self._property:
                raise ValueError(
                    '"schema_property" field is required and cannot be empty'
                )
            if not self._value:
                raise ValueError('"value" field is required and cannot be empty')

        def schema_change(self):
            return SchemaChange.set_property(self._property, self._value)

    @dataclass
    class RemoveSchemaPropertyRequest(SchemaUpdateRequestBase):
        """Represents a request to remove a property from a Schema."""

        _property: str = field(metadata=config(field_name="property"))
        """The property to remove."""

        def __init__(self, schema_property: str):
            super().__init__("removeProperty")
            self._property = schema_property

        def validate(self):
            """Validates the fields of the request.

            Raises:
                 IllegalArgumentException if schema_property is not set.
            """
            if not self._property:
                raise ValueError(
                    '"schema_property" field is required and cannot be empty'
                )

        def schema_change(self):
            return SchemaChange.remove_property(self._property)

    @dataclass
    class SetSchemaSecretBindingRequest(SchemaUpdateRequestBase):
        """Represents a request to bind a write-through secret for a schema property."""

        _property: Optional[str] = field(metadata=config(field_name="property"))
        _provider: Optional[str] = field(metadata=config(field_name="provider"))
        _plaintext: Optional[str] = field(metadata=config(field_name="plaintext"))

        def __init__(self, schema_property: str, provider: str, plaintext: str):
            super().__init__("setSecretBinding")
            self._property = schema_property
            self._provider = provider
            self._plaintext = plaintext

        def validate(self):
            if not self._property:
                raise ValueError('"property" field is required and cannot be empty')
            if not self._provider:
                raise ValueError('"provider" field is required and cannot be empty')
            if self._plaintext is None:
                raise ValueError('"plaintext" field is required and cannot be null')

        def schema_change(self):
            return SchemaChange.set_secret_binding(
                self._property,
                SecretBinding(self._provider, self._plaintext),
            )

    @dataclass
    class SetSchemaSecretReferenceRequest(SchemaUpdateRequestBase):
        """Represents a request to bind an external secret reference for a schema property."""

        _property: Optional[str] = field(metadata=config(field_name="property"))
        _provider: Optional[str] = field(metadata=config(field_name="provider"))
        _attributes: Optional[Dict[str, str]] = field(
            metadata=config(field_name="attributes")
        )

        def __init__(
            self,
            schema_property: str,
            provider: str,
            attributes: Optional[Dict[str, str]] = None,
        ):
            super().__init__("setSecretReference")
            self._property = schema_property
            self._provider = provider
            self._attributes = attributes

        def validate(self):
            if not self._property:
                raise ValueError('"property" field is required and cannot be empty')
            if not self._provider:
                raise ValueError('"provider" field is required and cannot be empty')

        def schema_change(self):
            return SchemaChange.set_secret_reference(
                self._property,
                SecretReference(self._provider, self._attributes or {}),
            )
