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
from typing import Dict

from dataclasses_json import config, DataClassJsonMixin

from gravitino.api.credential.credential import Credential


@dataclass
class CredentialDTO(Credential, DataClassJsonMixin):
    """Represents a Credential DTO (Data Transfer Object)."""

    _credential_type: str = field(metadata=config(field_name="credentialType"))
    _expire_time_in_ms: int = field(metadata=config(field_name="expireTimeInMs"))
    _credential_info: Dict[str, str] = field(
        metadata=config(field_name="credentialInfo")
    )

    def credential_type(self) -> str:
        return self._credential_type

    def expire_time_in_ms(self) -> int:
        return self._expire_time_in_ms

    def credential_info(self) -> Dict[str, str]:
        return self._credential_info
