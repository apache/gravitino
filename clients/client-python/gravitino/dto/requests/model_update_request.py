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
from typing import Optional

from dataclasses_json import config

from gravitino.api.model_change import ModelChange
from gravitino.rest.rest_message import RESTRequest


@dataclass
class ModelUpdateRequestBase(RESTRequest):
    """Base class for all model update requests."""

    _type: str = field(metadata=config(field_name="@type"))

    def __init__(self, action_type: str):
        self._type = action_type

    @abstractmethod
    def model_change(self) -> ModelChange:
        """Convert to model change operation"""
        pass


class ModelUpdateRequest:
    """Namespace for all model update request types."""

    @dataclass
    class UpdateModelNameRequest(ModelUpdateRequestBase):
        """Request to update model name"""

        _new_name: Optional[str] = field(metadata=config(field_name="newName"))

        def __init__(self, new_name: str):
            super().__init__("rename")
            self._new_name = new_name

        def validate(self):
            if not self._new_name:
                raise ValueError('"new_name" field is required')

        def model_change(self) -> ModelChange:
            return ModelChange.rename(self._new_name)

    @dataclass
    class ModelSetPropertyRequest(ModelUpdateRequestBase):
        """Request to set model property"""

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

        def model_change(self) -> ModelChange:
            return ModelChange.set_property(self._property, self._value)

    @dataclass
    class ModelRemovePropertyRequest(ModelUpdateRequestBase):
        """Request to remove model property"""

        _property: Optional[str] = field(metadata=config(field_name="property"))

        def __init__(self, pro: str):
            super().__init__("removeProperty")
            self._property = pro

        def validate(self):
            if not self._property:
                raise ValueError('"property" field is required')

        def model_change(self) -> ModelChange:
            return ModelChange.remove_property(self._property)

    @dataclass
    class UpdateModelCommentRequest(ModelUpdateRequestBase):
        """Request to update model comment"""

        _new_comment: Optional[str] = field(metadata=config(field_name="newComment"))
        """Represents a request to update the comment on a Metalake."""

        def __init__(self, new_comment: str):
            super().__init__("updateComment")
            self._new_comment = new_comment

        def validate(self):
            """Validates the fields of the request. Always pass."""
            pass

        def model_change(self):
            return ModelChange.update_comment(self._new_comment)
