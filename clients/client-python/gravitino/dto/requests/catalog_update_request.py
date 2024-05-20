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

    def __init__(self, type: str):
        self._type = type

    @abstractmethod
    def catalog_change(self):
        pass


class CatalogUpdateRequest:
    """Represents an interface for catalog update requests."""

    class RenameCatalogRequest(CatalogUpdateRequestBase):
        new_name: Optional[str] = field(metadata=config(field_name="newName"))

        def catalog_change(self):
            return CatalogChange.rename(self.new_name)

        def validate(self):
            """Validates the fields of the request.

            Raises:
                IllegalArgumentException if the new name is not set.
            """
            assert (
                self.new_name is None
            ), '"newName" field is required and cannot be empty'

    class UpdateCatalogCommentRequest(CatalogUpdateRequestBase):
        """Request to update the comment of a catalog."""

        new_comment: Optional[str] = field(metadata=config(field_name="newComment"))

        def catalog_change(self):
            return CatalogChange.update_comment(self.new_comment)

        def validate(self):
            assert (
                self.new_comment is None
            ), '"newComment" field is required and cannot be empty'

    class SetCatalogPropertyRequest(CatalogUpdateRequestBase):
        """Request to set a property on a catalog."""

        property: Optional[str] = None
        value: Optional[str] = None

        def catalog_change(self):
            return CatalogChange.set_property(self.property, self.value)

        def validate(self):
            assert (
                self.property is None
            ), '"property" field is required and cannot be empty'
            assert self.value is None, '"value" field is required and cannot be empty'

    class RemoveCatalogPropertyRequest(CatalogUpdateRequestBase):
        """Request to remove a property from a catalog."""

        property: Optional[str] = None

        def catalog_change(self):
            return CatalogChange.remove_property(self.property)

        def validate(self):
            assert (
                self.property is None
            ), '"property" field is required and cannot be empty'
