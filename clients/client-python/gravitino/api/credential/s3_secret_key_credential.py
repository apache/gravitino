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


class S3SecretKeyCredential(Credential, ABC):
    """Represents S3 secret key credential."""

    S3_SECRET_KEY_CREDENTIAL_TYPE: str = "s3-secret-key"
    _STATIC_ACCESS_KEY_ID: str = "s3-access-key-id"
    _STATIC_SECRET_ACCESS_KEY: str = "s3-secret-access-key"

    def __init__(self, credential_info: Dict[str, str], expire_time: int):
        self._access_key_id = credential_info.get(self._STATIC_ACCESS_KEY_ID, None)
        self._secret_access_key = credential_info.get(
            self._STATIC_SECRET_ACCESS_KEY, None
        )
        Precondition.check_string_not_empty(
            self._access_key_id, "S3 access key id should not be empty"
        )
        Precondition.check_string_not_empty(
            self._secret_access_key, "S3 secret access key should not be empty"
        )
        Precondition.check_argument(
            expire_time == 0,
            "The expiration time of S3 secret key credential should be 0",
        )

    def credential_type(self) -> str:
        """Returns the expiration time of the credential in milliseconds since
        the epoch, 0 means it will never expire.

        Returns:
             The expiration time of the credential.
        """
        return self.S3_SECRET_KEY_CREDENTIAL_TYPE

    def expire_time_in_ms(self) -> int:
        """Returns the expiration time of the credential in milliseconds since
        the epoch, 0 means it will never expire.

        Returns:
             The expiration time of the credential.
        """
        return 0

    def credential_info(self) -> Dict[str, str]:
        """The credential information.

        Returns:
             The credential information.
        """
        return {
            self._STATIC_ACCESS_KEY_ID: self._access_key_id,
            self._STATIC_SECRET_ACCESS_KEY: self._secret_access_key,
        }

    def access_key_id(self) -> str:
        """The S3 access key id.

        Returns:
            The S3 access key id.
        """
        return self._access_key_id

    def secret_access_key(self) -> str:
        """The S3 secret access key.

        Returns:
            The S3 secret access key.
        """
        return self._secret_access_key
