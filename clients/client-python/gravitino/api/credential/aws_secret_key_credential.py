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
from typing import Dict

from gravitino.api.credential.credential import Credential
from gravitino.utils.precondition import Precondition


class AwsSecretKeyCredential(Credential, ABC):
    """Represents AWS secret key credential for Glue API authentication."""

    AWS_SECRET_KEY_CREDENTIAL_TYPE: str = "aws-secret-key"
    _ACCESS_KEY_ID: str = "aws-access-key-id"
    _SECRET_ACCESS_KEY: str = "aws-secret-access-key"

    def __init__(self, credential_info: Dict[str, str], expire_time: int):
        self._access_key_id = credential_info.get(self._ACCESS_KEY_ID, None)
        self._secret_access_key = credential_info.get(self._SECRET_ACCESS_KEY, None)
        Precondition.check_string_not_empty(
            self._access_key_id, "AWS access key id should not be empty"
        )
        Precondition.check_string_not_empty(
            self._secret_access_key, "AWS secret access key should not be empty"
        )
        Precondition.check_argument(
            expire_time == 0,
            "The expiration time of AWS secret key credential should be 0",
        )

    def credential_type(self) -> str:
        return self.AWS_SECRET_KEY_CREDENTIAL_TYPE

    def expire_time_in_ms(self) -> int:
        return 0

    def credential_info(self) -> Dict[str, str]:
        return {
            self._ACCESS_KEY_ID: self._access_key_id,
            self._SECRET_ACCESS_KEY: self._secret_access_key,
        }

    def access_key_id(self) -> str:
        return self._access_key_id

    def secret_access_key(self) -> str:
        return self._secret_access_key
