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
from typing import Dict


@dataclass
class SecretBinding:
    """Write-through secret binding: provider instance name plus plaintext."""

    provider: str
    plaintext: str

    def __repr__(self) -> str:
        return f"SecretBinding(provider={self.provider!r}, plaintext=***)"


@dataclass
class SecretReference:
    """External secret locator: provider instance name plus provider-specific attributes."""

    provider: str
    attributes: Dict[str, str]

    def __post_init__(self):
        if self.attributes is None or len(self.attributes) == 0:
            raise ValueError("attributes must not be null or empty")

    def __repr__(self) -> str:
        return f"SecretReference(provider={self.provider!r}, attributes={self.attributes!r})"
