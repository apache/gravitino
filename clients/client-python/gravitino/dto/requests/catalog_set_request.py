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

from gravitino.rest.rest_message import RESTRequest


@dataclass
class CatalogSetRequest(RESTRequest):
    """Represents a request to set a catalog in use."""

    _in_use: bool = field(metadata=config(field_name="inUse"))

    def __init__(self, in_use: bool):
        self._in_use = in_use

    def validate(self):
        """Validates the fields of the request.

        Raises:
            IllegalArgumentException if in_use is not set.
        """
        if self._in_use is None:
            raise ValueError('"in_use" field is required and cannot be empty')
