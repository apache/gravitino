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
