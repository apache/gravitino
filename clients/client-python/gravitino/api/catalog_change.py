"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
"""

from abc import ABC


class CatalogChange(ABC):
    """
    A catalog change is a change to a catalog. It can be used to rename a catalog, update the comment
    of a catalog, set a property and value pair for a catalog, or remove a property from a catalog.
    """

    @staticmethod
    def rename(new_name):
        """Creates a new catalog change to rename the catalog.

        Args:
            new_name: The new name of the catalog.

        Returns:
            The catalog change.
        """
        return CatalogChange.RenameCatalog(new_name)

    @staticmethod
    def update_comment(new_comment):
        """Creates a new catalog change to update the catalog comment.

        Args:
            new_comment: The new comment for the catalog.

        Returns:
            The catalog change.
        """
        return CatalogChange.UpdateCatalogComment(new_comment)

    @staticmethod
    def set_property(catalog_property, value):
        """Creates a new catalog change to set the property and value for the catalog.

        Args:
            catalog_property: The property name to set.
            value: The value to set the property to.

        Returns:
            The catalog change.
        """
        return CatalogChange.SetProperty(catalog_property, value)

    @staticmethod
    def remove_property(catalog_property):
        """Creates a new catalog change to remove a property from the catalog.

        Args:
            catalog_property: The property name to remove.

        Returns:
            The catalog change.
        """
        return CatalogChange.RemoveProperty(catalog_property)

    class RenameCatalog:
        """A catalog change to rename the catalog."""

        def __init__(self, new_name):
            self._new_name = new_name

        def new_name(self):
            """Retrieves the new name set for the catalog.

            Returns:
                The new name of the catalog.
            """
            return self._new_name

        def __eq__(self, other) -> bool:
            """Compares this RenameCatalog instance with another object for equality. Two instances are
            considered equal if they designate the same new name for the catalog.

            Args:
                other: The object to compare with this instance.

            Returns:
                true if the given object represents an identical catalog renaming operation; false otherwise.
            """
            if not isinstance(other, CatalogChange.RenameCatalog):
                return False
            return self.new_name() == other.new_name()

        def __hash__(self):
            """Generates a hash code for this RenameCatalog instance. The hash code is primarily based on
            the new name for the catalog.

            Returns:
                A hash code value for this renaming operation.
            """
            return hash(self.new_name())

        def __str__(self):
            """Provides a string representation of the RenameCatalog instance. This string includes the
            class name followed by the new name of the catalog.

            Returns:
                A string summary of this renaming operation.
            """
            return f"RENAMECATALOG {self.new_name()}"

    class UpdateCatalogComment:
        """A catalog change to update the catalog comment."""

        def __init__(self, new_comment):
            self._new_comment = new_comment

        def new_comment(self):
            """Retrieves the new comment intended for the catalog.

            Returns:
                The new comment that has been set for the catalog.
            """
            return self._new_comment

        def __eq__(self, other) -> bool:
            """Compares this UpdateCatalogComment instance with another object for equality.
            Two instances are considered equal if they designate the same new comment for the catalog.

            Args:
                other: The object to compare with this instance.

            Returns:
                true if the given object represents the same comment update; false otherwise.
            """
            if not isinstance(other, CatalogChange.UpdateCatalogComment):
                return False
            return self.new_comment() == other.new_comment()

        def __hash__(self):
            """Generates a hash code for this UpdateCatalogComment instance.
            The hash code is based on the new comment for the catalog.

            Returns:
                A hash code representing this comment update operation.
            """
            return hash(self.new_comment())

        def __str__(self):
            """Provides a string representation of the UpdateCatalogComment instance.
            This string format includes the class name followed by the new comment for the catalog.

            Returns:
                A string summary of this comment update operation.
            """
            return f"UPDATECATALOGCOMMENT {self.new_comment()}"

    class SetProperty:
        """A catalog change to set the property and value for the catalog."""

        def __init__(self, catalog_property, value):
            self._property = catalog_property
            self._value = value

        def property(self):
            """Retrieves the name of the property being set in the catalog.

            Returns:
                The name of the property.
            """
            return self._property

        def value(self):
            """Retrieves the value assigned to the property in the catalog.

            Returns:
                The value of the property.
            """
            return self._value

        def __eq__(self, other) -> bool:
            """Compares this SetProperty instance with another object for equality.
            Two instances are considered equal if they have the same property and value for the catalog.

            Args:
                other: The object to compare with this instance.

            Returns:
                true if the given object represents the same property setting; false otherwise.
            """
            if not isinstance(other, CatalogChange.SetProperty):
                return False
            return self.property() == other.property() and self.value() == other.value()

        def __hash__(self):
            """Generates a hash code for this SetProperty instance.
            The hash code is based on both the property name and its assigned value.

            Returns:
                 A hash code value for this property setting.
            """
            return hash((self.property(), self.value()))

        def __str__(self):
            """Provides a string representation of the SetProperty instance.
            This string format includes the class name followed by the property and its value.

            Returns:
                 A string summary of the property setting.
            """
            return f"SETPROPERTY {self.property()} {self.value()}"

    class RemoveProperty:
        """A catalog change to remove a property from the catalog."""

        def __init__(self, catalog_property):
            self._property = catalog_property

        def get_property(self):
            """Retrieves the name of the property to be removed from the catalog.

            Returns:
                 The name of the property for removal.
            """
            return self._property

        def __eq__(self, other) -> bool:
            """Compares this RemoveProperty instance with another object for equality.
            Two instances are considered equal if they target the same property for removal from the catalog.

            Args:
                other The object to compare with this instance.

            Returns:
                true if the given object represents the same property removal; false otherwise.
            """
            if not isinstance(other, CatalogChange.RemoveProperty):
                return False
            return self._property == other._property

        def __hash__(self):
            """Generates a hash code for this RemoveProperty instance.
            The hash code is based on the property name that is to be removed from the catalog.

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
