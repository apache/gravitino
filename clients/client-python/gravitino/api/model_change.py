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
