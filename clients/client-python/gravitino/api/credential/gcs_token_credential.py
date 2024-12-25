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


class GCSTokenCredential(Credential, ABC):
    """Represents the GCS token credential."""

    GCS_TOKEN_CREDENTIAL_TYPE: str = "gcs-token"
    _GCS_TOKEN_NAME: str = "token"

    _expire_time_in_ms: int = 0

    def __init__(self, credential_info: Dict[str, str], expire_time_in_ms: int):
        self._token = credential_info.get(self._GCS_TOKEN_NAME, None)
        self._expire_time_in_ms = expire_time_in_ms
        Precondition.check_string_not_empty(
            self._token, "GCS token should not be empty"
        )
        Precondition.check_argument(
            self._expire_time_in_ms > 0,
            "The expiration time of GCS token credential should be greater than 0",
        )

    def credential_type(self) -> str:
        """The type of the credential.

        Returns:
             the type of the credential.
        """
        return self.GCS_TOKEN_CREDENTIAL_TYPE

    def expire_time_in_ms(self) -> int:
        """Returns the expiration time of the credential in milliseconds since
        the epoch, 0 means it will never expire.

        Returns:
             The expiration time of the credential.
        """
        return self._expire_time_in_ms

    def credential_info(self) -> Dict[str, str]:
        """The credential information.

        Returns:
             The credential information.
        """
        return {self._GCS_TOKEN_NAME: self._token}

    def token(self) -> str:
        """The GCS token.

        Returns:
            The GCS token.
        """
        return self._token
