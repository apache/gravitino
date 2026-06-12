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
from typing import Set


@dataclass
class DefaultSetting:
    default_gravitino_uri: str = "http://127.0.0.1:8090"
    default_transport: str = "stdio"
    default_mcp_url: str = "http://127.0.0.1:8000/mcp"


@dataclass
class Setting:  # pylint: disable=too-many-instance-attributes
    metalake: str
    gravitino_uri: str = DefaultSetting.default_gravitino_uri
    tags: Set[str] = field(default_factory=set)
    transport: str = DefaultSetting.default_transport
    mcp_url: str = DefaultSetting.default_mcp_url
    # Static OAuth2 Bearer token. Sent on every request in stdio mode; in HTTP
    # mode it is only the fallback used when an incoming request carries no
    # Authorization header (per-request identity takes priority).
    # Empty string means anonymous (no Authorization header sent).
    # repr=False keeps the raw value out of the dataclass-generated __repr__.
    token: str = field(default="", repr=False)
    # TLS certificate/key paths for serving the HTTP endpoint over HTTPS.
    # Both must be set to enable TLS; empty means plain HTTP.
    tls_cert: str = ""
    tls_key: str = ""

    def __str__(self) -> str:
        token_display = "***" if self.token else ""
        return (
            f"Setting(metalake={self.metalake}, gravitino_uri={self.gravitino_uri}, "
            f"tags={self.tags}, transport={self.transport}, mcp_url={self.mcp_url}, "
            f"token={token_display}, tls_cert={self.tls_cert}, "
            f"tls_key={self.tls_key})"
        )
