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
from types import MappingProxyType
from typing import Dict, Mapping, Optional

from dataclasses_json import config

from gravitino.api.catalog import Catalog
from gravitino.api.secret import SecretBinding, SecretReference
from gravitino.rest.rest_message import RESTRequest

_EMPTY_SECRET_BINDINGS: Mapping[str, SecretBinding] = MappingProxyType({})
_EMPTY_SECRET_REFERENCES: Mapping[str, SecretReference] = MappingProxyType({})


@dataclass
class CatalogCreateRequest(RESTRequest):
    """Represents a request to create a catalog."""

    _name: str = field(metadata=config(field_name="name"))
    _type: Catalog.Type = field(
        metadata=config(
            field_name="type",
            encoder=Catalog.Type.type_serialize,
            decoder=Catalog.Type.type_deserialize,
        )
    )
    _provider: Optional[str] = field(metadata=config(field_name="provider"))
    _comment: Optional[str] = field(metadata=config(field_name="comment"))
    _properties: Optional[Dict[str, str]] = field(
        metadata=config(field_name="properties")
    )
    _secret_bindings: Dict[str, SecretBinding] = field(
        default_factory=dict, metadata=config(field_name="secretBindings")
    )
    _secret_references: Dict[str, SecretReference] = field(
        default_factory=dict, metadata=config(field_name="secretReferences")
    )

    def __init__(
        self,
        name: str = None,
        catalog_type: Catalog.Type = Catalog.Type.UNSUPPORTED,
        provider: str = None,
        comment: str = None,
        properties: Dict[str, str] = None,
        secret_bindings: Mapping[str, SecretBinding] = _EMPTY_SECRET_BINDINGS,
        secret_references: Mapping[str, SecretReference] = _EMPTY_SECRET_REFERENCES,
    ):
        self._name = name
        self._type = catalog_type
        self._provider = provider
        self._comment = comment
        self._properties = properties
        self._secret_bindings = dict(secret_bindings)
        self._secret_references = dict(secret_references)

    def validate(self):
        """Validates the fields of the request.

        Raises:
            IllegalArgumentException if name or type are not set.
        """
        if not self._name:
            raise ValueError('"name" field is required and cannot be empty')
        if not self._type:
            raise ValueError('"catalog_type" field is required and cannot be empty')
        if not self._provider and not self._type.supports_managed_catalog:
            raise ValueError(
                '"provider" field is required and cannot be empty for catalog type '
                "that does not support managed catalog"
            )
