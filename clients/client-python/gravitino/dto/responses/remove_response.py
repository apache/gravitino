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

from __future__ import annotations

from dataclasses import dataclass, field

from dataclasses_json import config, dataclass_json

from gravitino.dto.responses.base_response import BaseResponse
from gravitino.utils.precondition import Precondition


@dataclass_json
@dataclass
class RemoveResponse(BaseResponse):
    """Represents a response for a remove operation."""

    _removed: bool = field(metadata=config(field_name="removed"))

    def removed(self) -> bool:
        return self._removed

    def validate(self) -> None:
        """Validates the response.

        Raises:
            IllegalArgumentException: If the removed field is not set.
        """
        Precondition.check_argument(
            self._removed is not None,
            "Remove response must contain 'removed' field",
        )
