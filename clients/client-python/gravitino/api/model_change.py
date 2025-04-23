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


class ModelChange(ABC):
    """
    A model change is a change to a model. It can be used to rename a model, update the comment
    of a model, set a property and value pair for a model, or remove a property from a model.
    """

    @staticmethod
    def rename(new_name):
        """Creates a new model change to rename the model.
        Args:
            new_name: The new name of the model.
        Returns:
            The model change.
        """
        return ModelChange.RenameModel(new_name)

    @staticmethod
    def set_property(pro, value):
        """Creates a new model change to set the property and value pairs for the model.
        Args:
            property: The name of the property to be set.
            value: The value of the property to be set.
        Returns:
            The model change.
        """
        return ModelChange.SetProperty(pro, value)

    @staticmethod
    def remove_property(pro):
        """Creates a new model change to remove the property and value pairs for the model.
        Args:
            property: The name of the property to be removed.
        Returns:
            The model change.
        """
        return ModelChange.RemoveProperty(pro)

    @staticmethod
    def update_comment(comment):
        """Creates a new model change to update the comment of the model.
        Args:
            comment: The new comment of the model.
        Returns:
            The model change.
        """
        return ModelChange.UpdateComment(comment)

    class RenameModel:
        """A model change to rename the model."""

        def __init__(self, new_name):
            self._new_name = new_name

        def new_name(self):
            """Retrieves the new name set for the model.
            Returns:
                The new name of the model.
            """
            return self._new_name

        def __eq__(self, other) -> bool:
            """Compares this RenameModel instance with another object for equality. Two instances are
            considered equal if they designate the same new name for the model.

            Args:
                other: The object to compare with this instance.

            Returns:
                true if the given object represents an identical model renaming operation; false otherwise.
            """
            if not isinstance(other, ModelChange.RenameModel):
                return False
            return self.new_name() == other.new_name()

        def __hash__(self):
            """Generates a hash code for this RenameModel instance. The hash code is primarily based on
            the new name for the model.

            Returns:
                A hash code value for this renaming operation.
            """
            return hash(self.new_name())

        def __str__(self):
            """Provides a string representation of the RenameModel instance. This string includes the
            class name followed by the new name of the model.

            Returns:
                A string summary of this renaming operation.
            """
            return f"RENAMEMODEL {self.new_name()}"

    class SetProperty:
        """
        A model change to set the property and value pairs for the model.
        """

        def __init__(self, pro, value):
            self._property = pro
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

        def __eq__(self, other) -> bool:
            """Compares this SetProperty instance with another object for equality. Two instances are
            considered equal if they designate the same property and value for the model.
            Args:
                other: The object to compare with this instance.
            Returns:
                true if the given object represents an identical model property setting operation; false otherwise.
            """
            if not isinstance(other, ModelChange.SetProperty):
                return False
            return self.property() == other.property() and self.value() == other.value()

        def __hash__(self):
            """Generates a hash code for this SetProperty instance. The hash code is primarily based on
            the property and value for the model.
            Returns:
                A hash code value for this property setting operation.
            """
            return hash(self.property(), self.value())

        def __str__(self):
            """Provides a string representation of the SetProperty instance. This string includes the
            class name followed by the property and value of the model.
            Returns:
                A string summary of this property setting operation.
            """
            return f"SETPROPERTY {self.property()}={self.value()}"

    class RemoveProperty:
        """
        A model change to remove the property and value pairs for the model.
        """

        def __init__(self, pro):
            self._property = pro

        def property(self):
            """Retrieves the name of the property to be removed.
            Returns:
                The name of the property.
            """
            return self._property

        def __eq__(self, other) -> bool:
            """Compares this RemoveProperty instance with another object for equality. Two instances are
            considered equal if they designate the same property for the model.
            Args:
                other: The object to compare with this instance.
            Returns:
                true if the given object represents an identical model property removal operation; false otherwise.
            """
            if not isinstance(other, ModelChange.RemoveProperty):
                return False
            return self.property() == other.property()

        def __hash__(self):
            """Generates a hash code for this RemoveProperty instance. The hash code is primarily based on
            the property for the model.
            Returns:
                A hash code value for this property removal operation.
            """
            return hash(self.property())

        def __str__(self):
            """Provides a string representation of the RemoveProperty instance. This string includes the
            class name followed by the property of the model.
            Returns:
                A string summary of this property removal operation.
            """
            return f"REMOVEPROPERTY {self.property()}"

    class UpdateComment:
        """
        A model change to update the comment of the model.
        """

        def __init__(self, new_comment):
            self._new_comment = new_comment

        def new_comment(self):
            """Retrieves the comment of the model.
            Returns:
                The comment of the model.
            """
            return self._new_comment

        def __eq__(self, other) -> bool:
            """Compares this UpdateComment instance with another object for equality. Two instances are
            considered equal if they designate the same comment for the model.
            Args:
                other: The object to compare with this instance.
            Returns:
                true if the given object represents an identical model comment update operation; false otherwise.
            """
            if not isinstance(other, ModelChange.UpdateComment):
                return False
            return self.new_comment() == other.new_comment()

        def __hash__(self):
            """Generates a hash code for this UpdateComment instance. The hash code is primarily based on
            the comment for the model.
            Returns:
                A hash code value for this comment update operation.
            """
            return hash(self.new_comment())

        def __str__(self):
            """Provides a string representation of the UpdateComment instance. This string includes the
            class name followed by the comment of the model.
            Returns:
                A string summary of this comment update operation.
            """
            return f"UpdateComment {self.new_comment()}"
