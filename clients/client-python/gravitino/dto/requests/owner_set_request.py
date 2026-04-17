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

from dataclasses_json import config, dataclass_json

from gravitino.api.authorization.owner import Owner
from gravitino.exceptions.base import IllegalArgumentException
from gravitino.rest.rest_message import RESTRequest


@dataclass_json
@dataclass
class OwnerSetRequest(RESTRequest):
    """Represents a request to set an owner for a metadata object."""

    _name: str = field(metadata=config(field_name="name"))
    _type: Owner.Type = field(metadata=config(field_name="type"))

    def __init__(self, name: str, owner_type: Owner.Type):
        self._name = name
        self._type = owner_type

    def validate(self) -> None:
        if not self._name:
            raise IllegalArgumentException('"name" field is required')
        if self._type is None:
            raise IllegalArgumentException('"type" field is required')
