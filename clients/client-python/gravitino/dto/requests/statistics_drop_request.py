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

from dataclasses_json import config

from gravitino.rest.rest_message import RESTRequest
from gravitino.utils.precondition import Precondition


@dataclass
class StatisticsDropRequest(RESTRequest):
    """Represents a request to drop statistics for specified names."""

    _names: list[str] = field(metadata=config(field_name="names"))

    @property
    def names(self) -> list[str]:
        return self._names

    def validate(self) -> None:
        Precondition.check_argument(
            self._names is not None and len(self._names) > 0,
            '"names" must not be null or empty',
        )
        for name in self._names:
            Precondition.check_string_not_empty(
                name, "Each name must be a non-empty string"
            )
