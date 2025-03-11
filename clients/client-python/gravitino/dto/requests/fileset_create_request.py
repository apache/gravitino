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
from typing import Optional, Dict

from dataclasses_json import config

from gravitino.api.file.fileset import Fileset
from gravitino.rest.rest_message import RESTRequest


@dataclass
class FilesetCreateRequest(RESTRequest):
    """Represents a request to create a fileset."""

    _name: str = field(metadata=config(field_name="name"))
    _comment: Optional[str] = field(metadata=config(field_name="comment"))
    _type: Optional[Fileset.Type] = field(metadata=config(field_name="type"))
    _storage_location: Optional[str] = field(
        metadata=config(field_name="storageLocation")
    )
    _properties: Optional[Dict[str, str]] = field(
        metadata=config(field_name="properties")
    )

    def __init__(
        self,
        name: str,
        comment: Optional[str] = None,
        fileset_type: Fileset.Type = None,
        storage_location: Optional[str] = None,
        properties: Optional[Dict[str, str]] = None,
    ):
        self._name = name
        self._comment = comment
        self._type = fileset_type
        self._storage_location = storage_location
        self._properties = properties

    def validate(self):
        """Validates the request.

        Raises:
            IllegalArgumentException if the request is invalid.
        """
        if not self._name:
            raise ValueError('"name" field is required and cannot be empty')
