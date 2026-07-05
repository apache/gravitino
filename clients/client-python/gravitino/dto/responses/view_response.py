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

from dataclasses import dataclass, field

from dataclasses_json import config

from gravitino.dto.rel.view_dto import ViewDTO
from gravitino.dto.responses.base_response import BaseResponse
from gravitino.exceptions.base import IllegalArgumentException


@dataclass
class ViewResponse(BaseResponse):
    """Response object for view-related operations."""

    _view: ViewDTO = field(metadata=config(field_name="view"))

    def view(self) -> ViewDTO:
        """Returns the view DTO."""
        return self._view

    def validate(self):
        """Validates the response data."""
        super().validate()
        if self._view is None:
            raise IllegalArgumentException("view must not be null")
        if not self._view.name():
            raise IllegalArgumentException("view 'name' must not be null or empty")
        if self._view.audit_info() is None:
            raise IllegalArgumentException("view 'audit' must not be null")
        if not self._view.representations():
            raise IllegalArgumentException("view 'representations' must not be null")
