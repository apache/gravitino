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

from dataclasses import dataclass
from enum import Enum
from typing import Optional

from gravitino.api.metadata_object import MetadataObject


@dataclass(frozen=True)
class DomainRelation:
    """One applicable relation to a domain, with no precedence over others."""

    class Type(Enum):
        """How the object belongs to the listed domain's scope."""

        DIRECT = "direct"
        INHERITED = "inherited"
        VIA_SUBDOMAIN = "via_subdomain"

    _type: Type
    _source: Optional[MetadataObject] = None

    def type(self) -> Type:
        """Return the relation kind."""
        return self._type

    def source(self) -> Optional[MetadataObject]:
        """Return the tagged catalog/schema or subdomain, or None for DIRECT."""
        return self._source
