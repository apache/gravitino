"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

from abc import abstractmethod
from dataclasses import field, dataclass
from typing import Optional

from dataclasses_json import config

from gravitino.api.catalog_change import CatalogChange
from gravitino.rest.rest_message import RESTRequest


@dataclass
class CatalogUpdateRequestBase(RESTRequest):
    _type: str = field(metadata=config(field_name="@type"))

    def __init__(self, action_type: str):
        self._type = action_type

    @abstractmethod
    def catalog_change(self):
        pass


class CatalogUpdateRequest:
    """Represents an interface for catalog update requests."""

    @dataclass
    class RenameCatalogRequest(CatalogUpdateRequestBase):
        """Represents a request to rename a catalog."""

        _new_name: Optional[str] = field(metadata=config(field_name="newName"))
        """The new name for the catalog."""

        def __init__(self, new_name: str):
            super().__init__("rename")
            self._new_name = new_name

        def catalog_change(self):
            return CatalogChange.rename(self._new_name)

        def validate(self):
            """Validates the fields of the request.

            Raises:
                IllegalArgumentException if the new name is not set.
            """
            assert (
                self._new_name is None
            ), '"newName" field is required and cannot be empty'

    @dataclass
    class UpdateCatalogCommentRequest(CatalogUpdateRequestBase):
        """Request to update the comment of a catalog."""

        _new_comment: Optional[str] = field(metadata=config(field_name="newComment"))
        """The new comment for the catalog."""

        def __init__(self, new_comment: str):
            super().__init__("updateComment")
            self._new_comment = new_comment

        def catalog_change(self):
            return CatalogChange.update_comment(self._new_comment)

        def validate(self):
            assert (
                self._new_comment is None
            ), '"newComment" field is required and cannot be empty'

    @dataclass
    class SetCatalogPropertyRequest(CatalogUpdateRequestBase):
        """Request to set a property on a catalog."""

        _property: Optional[str] = field(metadata=config(field_name="property"))
        """The property to set."""

        _value: Optional[str] = field(metadata=config(field_name="value"))
        """The value of the property."""

        def __init__(self, catalog_property: str, value: str):
            super().__init__("setProperty")
            self._property = catalog_property
            self._value = value

        def catalog_change(self):
            return CatalogChange.set_property(self._property, self._value)

        def validate(self):
            assert (
                self._property is None
            ), '"property" field is required and cannot be empty'
            assert self._value is None, '"value" field is required and cannot be empty'

    class RemoveCatalogPropertyRequest(CatalogUpdateRequestBase):
        """Request to remove a property from a catalog."""

        property: Optional[str] = None
        """The property to remove."""

        def __init__(self, catalog_property: str):
            super().__init__("removeProperty")
            self._property = catalog_property

        def catalog_change(self):
            return CatalogChange.remove_property(self._property)

        def validate(self):
            assert (
                self._property is None
            ), '"property" field is required and cannot be empty'
