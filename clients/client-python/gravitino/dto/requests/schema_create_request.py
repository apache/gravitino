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

from gravitino.api.secret import SecretBinding, SecretReference
from gravitino.rest.rest_message import RESTRequest


@dataclass
class SchemaCreateRequest(RESTRequest):
    """Represents a request to create a schema."""

    _name: str = field(metadata=config(field_name="name"))
    _comment: Optional[str] = field(metadata=config(field_name="comment"))
    _properties: Optional[Dict[str, str]] = field(
        metadata=config(field_name="properties")
    )
    _secret_bindings: Optional[Dict[str, SecretBinding]] = field(
        default=None, metadata=config(field_name="secretBindings")
    )
    _secret_references: Optional[Dict[str, SecretReference]] = field(
        default=None, metadata=config(field_name="secretReferences")
    )

    def __init__(
        self,
        name: str,
        comment: Optional[str],
        properties: Optional[Dict[str, str]],
        secret_bindings: Optional[Dict[str, SecretBinding]] = None,
        secret_references: Optional[Dict[str, SecretReference]] = None,
    ):
        self._name = name
        self._comment = comment
        self._properties = properties
        self._secret_bindings = secret_bindings
        self._secret_references = secret_references

    def validate(self):
        if not self._name:
            raise ValueError('"name" field is required and cannot be empty')
