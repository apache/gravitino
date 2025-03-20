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
from dataclasses import dataclass, field
from dataclasses_json import config

class ModelChange(ABC):
    """A model change is a change to a model. It can be used to update model metadata."""

    @staticmethod
    def update_comment(new_comment: str):
        """Creates a new model change to update the model comment.

        Args:
            new_comment: The new comment for the model.

        Returns:
            The model change.
        """
        return ModelChange.UpdateModelComment(new_comment)

    @dataclass
    class UpdateModelComment:
        """A model change to update the model comment."""

        _new_comment: str = field(metadata=config(field_name="newComment"))

        def new_comment(self) -> str:
            """Retrieves the new comment for the model.

            Returns:
                The new comment that has been set.
            """
            return self._new_comment

        def __eq__(self, other) -> bool:
            if not isinstance(other, ModelChange.UpdateModelComment):
                return False
            return self._new_comment == other.new_comment()

        def __hash__(self):
            return hash(self._new_comment)

        def __str__(self):
            return f"UPDATEMODELCOMMENT {self._new_comment}"
