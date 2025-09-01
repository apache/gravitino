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
from dataclasses import field, dataclass
from typing import Optional, List, Dict

from dataclasses_json import config

from gravitino.exceptions.base import IllegalArgumentException
from gravitino.rest.rest_message import RESTRequest


@dataclass
class ModelVersionLinkRequest(RESTRequest):
    """Represents a request to link a model version to a model."""

    _uris: Dict[str, str] = field(metadata=config(field_name="uris"))
    _comment: Optional[str] = field(metadata=config(field_name="comment"))
    _aliases: Optional[List[str]] = field(metadata=config(field_name="aliases"))
    _properties: Optional[Dict[str, str]] = field(
        metadata=config(field_name="properties")
    )

    def __init__(
        self,
        uris: Dict[str, str],
        comment: Optional[str] = None,
        aliases: Optional[List[str]] = None,
        properties: Optional[Dict[str, str]] = None,
    ):
        self._uris = uris
        self._comment = comment
        self._aliases = aliases
        self._properties = properties

    def validate(self):
        """Validates the request.

        Raises:
            IllegalArgumentException if the request is invalid
        """
        if not self._uris:
            raise IllegalArgumentException(
                '"uris" field is required and cannot be empty'
            )

        for key, value in self._uris.items():
            if not self._is_not_blank(key):
                raise IllegalArgumentException("uri name must not be null or empty")
            if not self._is_not_blank(value):
                raise IllegalArgumentException("uri must not be null or empty")

        for alias in self._aliases or []:
            if not self._is_not_blank(alias):
                raise IllegalArgumentException("Alias must not be null or empty")

    def _is_not_blank(self, string: str) -> bool:
        return string is not None and string.strip()
