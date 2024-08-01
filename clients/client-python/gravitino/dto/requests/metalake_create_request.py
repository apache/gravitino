"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
"""

from typing import Optional, Dict

from dataclasses import dataclass, field

from dataclasses_json import config

from gravitino.rest.rest_message import RESTRequest


@dataclass
class MetalakeCreateRequest(RESTRequest):
    """Represents a request to create a Metalake."""

    _name: str = field(metadata=config(field_name="name"))
    _comment: Optional[str] = field(metadata=config(field_name="comment"))
    _properties: Optional[Dict[str, str]] = field(
        metadata=config(field_name="properties")
    )

    def __init__(
        self, name: str = None, comment: str = None, properties: Dict[str, str] = None
    ):
        super().__init__()

        self._name = name.strip() if name else None
        self._comment = comment.strip() if comment else None
        self._properties = properties

    def name(self) -> str:
        return self._name

    def validate(self):
        if not self._name:
            raise ValueError('"name" field is required and cannot be empty')
