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

from gravitino.api.model.model_change import ModelChange
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
            super().__init__("updateName")
            self._new_name = new_name

        def validate(self):
            if not self._new_name:
                raise ValueError('"new_name" field is required')

        def model_change(self) -> ModelChange:
            return ModelChange.rename(self._new_name)
