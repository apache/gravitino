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
class Setting:
    metalake: str
    gravitino_uri: str = DefaultSetting.default_gravitino_uri
    tags: Set[str] = field(default_factory=set)
    transport: str = DefaultSetting.default_transport
    mcp_url: str = DefaultSetting.default_mcp_url
