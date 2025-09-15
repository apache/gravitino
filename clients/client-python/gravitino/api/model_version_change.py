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

from abc import ABC


class ModelVersionChange(ABC):
    """
    A model version change is a change to a model version. It can be used to update the comment
    of a model version, set a property and value pair for a model version, or remove a property from a model.
    """

    @staticmethod
    def update_comment(comment: str):
        """Creates a new model version change to update the comment of the model version.
        Args:
            comment: The new comment of the model version.
        Returns:
            The model version change.
        """
        return ModelVersionChange.UpdateComment(comment)

    @staticmethod
    def set_property(key, value):
        """Creates a new model version change to set a property and value pair for the model version.
        Args:
            key: The key of the property.
            value: The value of the property.
        Returns:
            The model version change.
        """
        return ModelVersionChange.SetProperty(key, value)

    @staticmethod
    def remove_property(key: str):
        """Creates a new model version change to remove a property from the model version.
        Args:
            key: The key of the property.
        Returns:
            The model version change.
        """
        return ModelVersionChange.RemoveProperty(key)

    @staticmethod
    def update_uri(uri: str, uri_name: str = None):
        """Creates a new model version change to update the uri of the model version.
        Args:
            uri: The new uri of the model version.
            uri_name: The uri name of the model version to be updated.
        Returns:
            The model version change.
        """
        return ModelVersionChange.UpdateUri(uri, uri_name)

    @staticmethod
    def add_uri(uri_name: str, uri: str):
        """Creates a new model version change to add the uri of the model version.
        Args:
            uri_name: The uri name of the model version to be added.
            uri: The new uri of the model version to be added.
        Returns:
            The model version change.
        """
        return ModelVersionChange.AddUri(uri_name, uri)

    @staticmethod
    def remove_uri(uri_name: str):
        """Creates a new model version change to remove the uri of the model version.
        Args:
            uri_name: The uri name of the model version to be removed.
        Returns:
            The model version change.
        """
        return ModelVersionChange.RemoveUri(uri_name)

    @staticmethod
    def update_aliases(aliases_to_add, aliases_to_remove):
        """Creates a new model version change to update the aliases of the model version.
        Args:
            aliases_to_add: The new aliases to add to the model version.
            aliases_to_remove: The aliases to remove from the model version.
        Returns:
            The model version change.
        """
        return ModelVersionChange.UpdateAliases(aliases_to_add, aliases_to_remove)

    class UpdateComment:
        """A model version change to update the comment of the model version."""

        def __init__(self, new_comment: str):
            self._new_comment = new_comment

        def new_comment(self) -> str:
            """Retrieves the new comment of the model version.
            Returns:
                The new comment of the model version.
            """
            return self._new_comment

        def __eq__(self, other):
            """Compares this UpdateComment instance with another object for equality. Two instances are
            considered equal if they designate the same new comment for the model version.
            Args:
                other: The object to compare with this instance.
            Returns:
                true if the given object represents an identical model version comment update operation;
                false otherwise.
            """
            if not isinstance(other, ModelVersionChange.UpdateComment):
                return False

            return self.new_comment() == other.new_comment()

        def __hash__(self):
            """Generates a hash code for this UpdateComment instance. The hash code is primarily based on
            the new comment for the model version.
            Returns:
                A hash code value for this comment update operation.
            """
            return hash(self.new_comment())

        def __str__(self):
            """Provides a string representation of the UpdateComment instance. This string includes the
            class name followed by the new comment of the model version.
            Returns:
                A string summary of this comment update operation.
            """
            return f"UpdateComment {self._new_comment}"

    class SetProperty:
        """A model version change to set a property and value pair for the model version."""

        def __init__(self, key: str, value: str):
            self._key = key
            self._value = value

        def property(self) -> str:
            """Retrieves the name of the property.
            Returns:
                The name of the property.
            """
            return self._key

        def value(self) -> str:
            """Retrieves the value of the property.
            Returns:
                The value of the property.
            """
            return self._value

        def __eq__(self, other):
            """Compares this SetProperty instance with another object for equality. Two instances are
            considered equal if they designate the same key and value pair for the model version.
            Args:
                other: The object to compare with this instance.
            Returns:
                true if the given object represents an identical model version property set operation;  false otherwise.
            """
            if not isinstance(other, ModelVersionChange.SetProperty):
                return False
            return self.key() == other.key() and self.value() == other.value()

        def __hash__(self):
            """Generates a hash code for this SetProperty instance. The hash code is primarily based on
            the key and value pair for the model version.
            Returns:
                A hash code value for this property set operation.
            """
            return hash((self.key(), self.value()))

        def __str__(self):
            """Provides a string representation of the SetProperty instance. This string includes the
            class name followed by the key and value pair for the model version.
            Returns:
                A string summary of this property set operation.
            """
            return f"SetProperty {self.key()}={self.value()}"

    class RemoveProperty:
        """A model version change to remove a property from the model version."""

        def __init__(self, key: str):
            self._key = key

        def property(self) -> str:
            """Retrieves the name of the property.
            Returns:
                The name of the property.
            """
            return self._key

        def __eq__(self, other):
            """Compares this RemoveProperty instance with another object for equality. Two instances are
            considered equal if they designate the same key for the model version.
            Args:
                other: The object to compare with this instance.
            Returns:
                true if the given object represents an identical model version property remove operation;
                false otherwise.
            """
            if not isinstance(other, ModelVersionChange.RemoveProperty):
                return False
            return self.key() == other.key()

        def __hash__(self):
            """Generates a hash code for this RemoveProperty instance. The hash code is primarily based on
            the key for the model version.
            Returns:
                A hash code value for this property remove operation.
            """
            return hash(self.key())

        def __str__(self):
            """Provides a string representation of the RemoveProperty instance. This string includes the
            class name followed by the key for the model version.
            Returns:
                A string summary of this property remove operation.
            """
            return f"RemoveProperty {self.key()}"

    class UpdateUri:
        """A model version change to update the URI of the model version."""

        def __init__(self, new_uri: str, uri_name: str = None):
            self._new_uri = new_uri
            self._uri_name = uri_name

        def new_uri(self) -> str:
            """Retrieves the new URI of the model version.
            Returns:
                The new URI of the model version.
            """
            return self._new_uri

        def uri_name(self) -> str:
            """Retrieves the URI name of the model version to be updated.
            Returns:
                The URI name of the model version to be updated.
            """
            return self._uri_name

        def __eq__(self, other):
            """Compares this UpdateUri instance with another object for equality. Two instances are
            considered equal if they designate the same new URI and URI name for the model version.
            Args:
                other: The object to compare with this instance.
            Returns:
                true if the given object represents an identical model version URI update operation;
                false otherwise.
            """
            if not isinstance(other, ModelVersionChange.UpdateUri):
                return False
            return (
                self.new_uri() == other.new_uri()
                and self.uri_name() == other.uri_name()
            )

        def __hash__(self):
            """Generates a hash code for this UpdateUri instance. The hash code is primarily based on
            the new URI and its name for the model version.
            Returns:
                A hash code value for this URI update operation.
            """
            return hash((self.new_uri(), self.uri_name()))

        def __str__(self):
            """Provides a string representation of the UpdateUri instance. This string includes the
            class name followed by the new URI and its name of the model version.
            Returns:
                A string summary of this URI update operation.
            """
            return f"UpdateUri uriName: ({self._uri_name}) newUri: ({self._new_uri})"

    class AddUri:
        """A ModelVersionChange to add a uri of the model version."""

        def __init__(self, uri_name: str, uri: str):
            self._uri_name = uri_name
            self._uri = uri

        def uri_name(self) -> str:
            """Retrieves the URI name of the model version to be added.
            Returns:
                The URI name of the model version to be added.
            """
            return self._uri_name

        def uri(self) -> str:
            """Retrieves the URI of the model version to be added.
            Returns:
                The new URI of the model version to be added.
            """
            return self._uri

        def __eq__(self, other):
            """Compares this AddUri instance with another object for equality. Two instances are
            considered equal if they designate the same URI and URI name for the model version.
            Args:
                other: The object to compare with this instance.
            Returns:
                true if the given object represents an identical model version URI add operation;
                false otherwise.
            """
            if not isinstance(other, ModelVersionChange.AddUri):
                return False
            return self.uri_name() == other.uri_name() and self.uri() == other.uri()

        def __hash__(self):
            """Generates a hash code for this AddUri instance. The hash code is primarily based on
            the URI and its name for the model version.
            Returns:
                A hash code value for this URI add operation.
            """
            return hash((self.uri_name(), self.uri()))

        def __str__(self):
            """Provides a string representation of the AddUri instance. This string includes the
            class name followed by the URI and its name of the model version.
            Returns:
                A string summary of this URI add operation.
            """
            return f"AddUri uriName: ({self._uri_name}) uri: ({self.uri})"

    class RemoveUri:
        """A ModelVersionChange to remove a uri of the model version."""

        def __init__(self, uri_name: str):
            self._uri_name = uri_name

        def uri_name(self) -> str:
            """Retrieves the URI name of the model version to be removed.
            Returns:
                The URI name of the model version to be removed.
            """
            return self._uri_name

        def __eq__(self, other):
            """Compares this RemoveUri instance with another object for equality. Two instances are
            considered equal if they designate the same URI name for the model version.
            Args:
                other: The object to compare with this instance.
            Returns:
                true if the given object represents an identical model version URI remove operation;
                false otherwise.
            """
            if not isinstance(other, ModelVersionChange.RemoveUri):
                return False
            return self.uri_name() == other.uri_name()

        def __hash__(self):
            """Generates a hash code for this RemoveUri instance. The hash code is primarily based on
            the URI name for the model version.
            Returns:
                A hash code value for this URI remove operation.
            """
            return hash(self.uri_name())

        def __str__(self):
            """Provides a string representation of the RemoveUri instance. This string includes the
            class name followed by the URI name of the model version.
            Returns:
                A string summary of this URI remove operation.
            """
            return f"RemoveUri uriName: ({self._uri_name})"

    class UpdateAliases:
        """A model version change to update the aliases of the model version."""

        def __init__(self, aliases_to_add, aliases_to_remove):
            self._aliases_to_add = set(aliases_to_add) if aliases_to_add else set()
            self._aliases_to_remove = (
                set(aliases_to_remove) if aliases_to_remove else set()
            )

        def aliases_to_add(self):
            """Retrieves the aliases to add.
            Returns:
                The aliases to add.
            """
            return self._aliases_to_add

        def aliases_to_remove(self):
            """Retrieves the aliases to remove.
            Returns:
                The aliases to remove.
            """
            return self._aliases_to_remove

        def __eq__(self, other) -> bool:
            """Compares this UpdateAliases instance with another object for equality. Two instances are
            considered equal if they designate the same aliases to add and remove from the model version.
            Args:
                other: The object to compare with this instance.
            Returns:
                true if the given object represents an identical model version alias update operation;
                false otherwise.
            """
            if not isinstance(other, ModelVersionChange.UpdateAliases):
                return False
            return (
                self.aliases_to_add() == other.aliases_to_add()
                and self.aliases_to_remove() == other.aliases_to_remove()
            )

        def __hash__(self) -> int:
            """Generates a hash code for this UpdateAliases instance. The hash code is primarily based on
            the aliases to add and remove from the model version.
            Returns:
                A hash code value for this alias update operation.
            """
            return hash((self._aliases_to_add, self._aliases_to_remove))

        def __str__(self) -> str:
            """Returns a string representation of the UpdateAliases instance. This string includes the
            class name followed by the aliases to add and remove for the model version.
            Returns:
                A string summary of this alias update operation.
            """
            add_str = ", ".join(sorted(self._aliases_to_add))
            remove_str = ", ".join(sorted(self._aliases_to_remove))
            return f"UpdateAlias AliasToAdd: ({add_str}) AliasToRemove: ({remove_str})"
