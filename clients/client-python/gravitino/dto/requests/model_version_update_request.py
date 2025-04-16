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

from gravitino.api.model_version_change import ModelVersionChange
from gravitino.rest.rest_message import RESTRequest


@dataclass
class ModelVersionUpdateRequestBase(RESTRequest):
    """Base class for all model version update requests."""

    _type: str = field(metadata=config(field_name="@type"))

    def __init__(self, action_type: str):
        self._type = action_type

    @abstractmethod
    def model_version_change(self) -> ModelVersionChange:
        """Convert to model version change operation"""
        pass


class ModelVersionUpdateRequest:
    """Namespace for all model version update request types."""

    @dataclass
    class UpdateModelVersionComment(ModelVersionUpdateRequestBase):
        """Request to update model version comment"""

        _new_comment: Optional[str] = field(metadata=config(field_name="newComment"))
        """Represents a request to update the comment on a Metalake."""

        def __init__(self, new_comment: str):
            super().__init__("updateComment")
            self._new_comment = new_comment

        def validate(self):
            """Validates the fields of the request. Always pass."""
            pass

        def model_version_change(self):
            return ModelVersionChange.update_comment(self._new_comment)
