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
from typing import Optional, Set

from dataclasses_json import config

from gravitino.api.model_version_change import ModelVersionChange
from gravitino.rest.rest_message import RESTRequest


@dataclass
class ModelVersionUpdateRequestBase(RESTRequest):
    """Base class for all model version update requests."""

    _type: str = field(metadata=config(field_name="@type"))

    def __init__(self, action_type: str):
        self._type = action_type

    @abstractmethod
    def model_version_change(self) -> ModelVersionChange:
        """Convert to model version change operation"""
        pass


class ModelVersionUpdateRequest:
    """Namespace for all model version update request types."""

    @dataclass
    class UpdateModelVersionComment(ModelVersionUpdateRequestBase):
        """Request to update model version comment"""

        _new_comment: Optional[str] = field(metadata=config(field_name="newComment"))
        """Represents a request to update the comment on a Metalake."""

        def __init__(self, new_comment: str):
            super().__init__("updateComment")
            self._new_comment = new_comment

        def validate(self):
            """Validates the fields of the request. Always pass."""
            pass

        def model_version_change(self):
            return ModelVersionChange.update_comment(self._new_comment)

    @dataclass
    class SetModelVersionPropertyRequest(ModelVersionUpdateRequestBase):
        """Request to update model version properties"""

        _property: Optional[str] = field(metadata=config(field_name="property"))
        _value: Optional[str] = field(metadata=config(field_name="value"))

        def __init__(self, pro: str, value: str):
            super().__init__("setProperty")
            self._property = pro
            self._value = value

        def validate(self):
            if not self._property:
                raise ValueError('"property" field is required')
            if not self._value:
                raise ValueError('"value" field is required')

        def model_version_change(self) -> ModelVersionChange:
            return ModelVersionChange.set_property(self._property, self._value)

    @dataclass
    class RemoveModelVersionPropertyRequest(ModelVersionUpdateRequestBase):
        """Request to remove model version properties"""

        _property: Optional[str] = field(metadata=config(field_name="property"))

        def __init__(self, pro: str):
            super().__init__("removeProperty")
            self._property = pro

        def key(self):
            """
            Returns the key of the property to remove.
            Returns:
                str: The key of the property to remove.
            """
            return self._property

        def validate(self):
            if not self._property:
                raise ValueError('"property" field is required')

        def model_version_change(self):
            return ModelVersionChange.remove_property(self._property)

    @dataclass
    class UpdateModelVersionUriRequest(ModelVersionUpdateRequestBase):
        """Request to update model version uri"""

        _new_uri: Optional[str] = field(metadata=config(field_name="newUri"))
        _uri_name: Optional[str] = field(metadata=config(field_name="uriName"))

        def __init__(self, new_uri: str, uri_name: str):
            super().__init__("updateUri")
            self._new_uri = new_uri
            self._uri_name = uri_name

        def new_uri(self):
            """Retrieves the new uri of the model version.
            Returns:
                The new uri of the model version.
            """
            return self._new_uri

        def uri_name(self):
            """Retrieves the uri name of the model version.
            Returns:
                The uri name of the model version.
            """
            return self._uri_name

        def validate(self):
            """Validates the fields of the request."""
            if not self._new_uri:
                raise ValueError('"newUri" field is required')

        def model_version_change(self):
            """
            Returns a ModelVersionChange object representing the update uri operation.
            Returns:
                ModelVersionChange: The ModelVersionChange object representing the update uri operation.
            """
            return ModelVersionChange.update_uri(self._new_uri, self._uri_name)

    @dataclass
    class AddModelVersionUriRequest(ModelVersionUpdateRequestBase):
        """Request to add model version uri"""

        _uri_name: Optional[str] = field(metadata=config(field_name="uriName"))
        _uri: Optional[str] = field(metadata=config(field_name="uri"))

        def __init__(self, uri_name: str, uri: str):
            super().__init__("addUri")
            self._uri_name = uri_name
            self._uri = uri

        def uri_name(self):
            """Retrieves the uri name of the model version.
            Returns:
                The uri name of the model version.
            """
            return self._uri_name

        def uri(self):
            """Retrieves the uri of the model version.
            Returns:
                The uri of the model version.
            """
            return self._uri

        def validate(self):
            """Validates the fields of the request."""
            if not self._uri_name:
                raise ValueError('"uriName" field is required')
            if not self._uri:
                raise ValueError('"uri" field is required')

        def model_version_change(self):
            """
            Returns a ModelVersionChange object representing the add uri operation.
            Returns:
                ModelVersionChange: The ModelVersionChange object representing the add uri operation.
            """
            return ModelVersionChange.add_uri(self._uri_name, self._uri)

    @dataclass
    class RemoveModelVersionUriRequest(ModelVersionUpdateRequestBase):
        """Request to remove model version uri"""

        _uri_name: Optional[str] = field(metadata=config(field_name="uriName"))

        def __init__(self, uri_name: str):
            super().__init__("removeUri")
            self._uri_name = uri_name

        def uri_name(self):
            """Retrieves the uri name of the model version.
            Returns:
                The uri name of the model version.
            """
            return self._uri_name

        def validate(self):
            """Validates the fields of the request."""
            if not self._uri_name:
                raise ValueError('"uriName" field is required')

        def model_version_change(self):
            """
            Returns a ModelVersionChange object representing the remove uri operation.
            Returns:
                ModelVersionChange: The ModelVersionChange object representing the remove uri operation.
            """
            return ModelVersionChange.remove_uri(self._uri_name)

    @dataclass
    class ModelVersionAliasesRequest(ModelVersionUpdateRequestBase):
        """Request to update model version aliases"""

        _aliases_to_add: Optional[Set[str]] = field(
            metadata=config(field_name="aliasesToAdd")
        )
        _aliases_to_remove: Optional[Set[str]] = field(
            metadata=config(field_name="aliasesToRemove")
        )

        def __init__(self, aliases_to_add: Set[str], aliases_to_remove: Set[str]):
            super().__init__("updateAliases")
            self._aliases_to_add = aliases_to_add
            self._aliases_to_remove = aliases_to_remove

        def aliases_to_add(self):
            """Retrieves the new aliases of the model version.
            Returns:
                The new aliases of the model version.
            """
            return self._aliases_to_add

        def aliases_to_remove(self):
            """Retrieves the new aliases of the model version.
            Returns:
                The new aliases of the model version.
            """
            return self._aliases_to_remove

        def validate(self):
            """Validates the fields of the request."""
            if self._aliases_to_add is None and self._aliases_to_remove is None:
                raise ValueError(
                    "At least one of aliasesToAdd or aliasesToRemove must be non-null"
                )

        def model_version_change(self):
            """
            Returns a ModelVersionChange object representing the update aliases operation.
            Returns:
                ModelVersionChange: The ModelVersionChange object representing the update aliases operation.
            """
            return ModelVersionChange.UpdateAliases(
                self._aliases_to_add,
                self._aliases_to_remove,
            )
