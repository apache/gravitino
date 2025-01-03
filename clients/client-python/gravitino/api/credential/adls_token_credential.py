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


class ADLSTokenCredential(Credential, ABC):
    """Represents ADLS token credential."""

    ADLS_TOKEN_CREDENTIAL_TYPE: str = "adls-token"
    ADLS_DOMAIN: str = "dfs.core.windows.net"
    _STORAGE_ACCOUNT_NAME: str = "azure-storage-account-name"
    _SAS_TOKEN: str = "adls-sas-token"

    def __init__(self, credential_info: Dict[str, str], expire_time_in_ms: int):
        self._account_name = credential_info.get(self._STORAGE_ACCOUNT_NAME, None)
        self._sas_token = credential_info.get(self._SAS_TOKEN, None)
        self._expire_time_in_ms = expire_time_in_ms
        Precondition.check_string_not_empty(
            self._account_name, "The ADLS account name should not be empty."
        )
        Precondition.check_string_not_empty(
            self._sas_token, "The ADLS SAS token should not be empty."
        )
        Precondition.check_argument(
            self._expire_time_in_ms > 0,
            "The expiration time of ADLS token credential should be greater than 0",
        )

    def credential_type(self) -> str:
        """The type of the credential.

        Returns:
             the type of the credential.
        """
        return self.ADLS_TOKEN_CREDENTIAL_TYPE

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
        return {
            self._STORAGE_ACCOUNT_NAME: self._account_name,
            self._SAS_TOKEN: self._sas_token,
        }

    def account_name(self) -> str:
        """The ADLS account name.

        Returns:
            The ADLS account name.
        """
        return self._account_name

    def sas_token(self) -> str:
        """The ADLS sas token.

        Returns:
            The ADLS sas token.
        """
        return self._sas_token
