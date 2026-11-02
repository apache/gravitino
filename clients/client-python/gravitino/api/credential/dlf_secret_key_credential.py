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

from abc import ABC
from typing import Dict, Optional

from gravitino.api.credential.credential import Credential
from gravitino.utils.precondition import Precondition


class DlfSecretKeyCredential(Credential, ABC):
    """Represents DLF secret key credential for Paimon DLF catalogs."""

    DLF_SECRET_KEY_CREDENTIAL_TYPE: str = "dlf-secret-key"
    _ACCESS_KEY_ID: str = "dlf-access-key-id"
    _ACCESS_KEY_SECRET: str = "dlf-access-key-secret"
    _SECURITY_TOKEN: str = "dlf-security-token"

    def __init__(self, credential_info: Dict[str, str], expire_time: int):
        self._access_key_id = credential_info.get(self._ACCESS_KEY_ID, None)
        self._access_key_secret = credential_info.get(self._ACCESS_KEY_SECRET, None)
        self._security_token = credential_info.get(self._SECURITY_TOKEN, None)
        Precondition.check_string_not_empty(
            self._access_key_id, "DLF access key id should not be empty"
        )
        Precondition.check_string_not_empty(
            self._access_key_secret, "DLF access key secret should not be empty"
        )
        Precondition.check_argument(
            expire_time == 0,
            "The expiration time of DLF secret key credential should be 0",
        )

    def credential_type(self) -> str:
        return self.DLF_SECRET_KEY_CREDENTIAL_TYPE

    def expire_time_in_ms(self) -> int:
        return 0

    def credential_info(self) -> Dict[str, str]:
        info = {
            self._ACCESS_KEY_ID: self._access_key_id,
            self._ACCESS_KEY_SECRET: self._access_key_secret,
        }
        if self._security_token:
            info[self._SECURITY_TOKEN] = self._security_token
        return info

    def access_key_id(self) -> str:
        return self._access_key_id

    def access_key_secret(self) -> str:
        return self._access_key_secret

    def security_token(self) -> Optional[str]:
        return self._security_token
