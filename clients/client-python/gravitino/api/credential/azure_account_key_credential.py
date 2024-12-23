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


class AzureAccountKeyCredential(Credential, ABC):
    """Represents Azure account key credential."""

    AZURE_ACCOUNT_KEY_CREDENTIAL_TYPE: str = "azure-account-key"
    _STORAGE_ACCOUNT_NAME: str = "azure-storage-account-name"
    _STORAGE_ACCOUNT_KEY: str = "azure-storage-account-key"

    def __init__(self, credential_info: Dict[str, str], expire_time_in_ms: int):
        self._account_name = credential_info.get(self._STORAGE_ACCOUNT_NAME, None)
        self._account_key = credential_info.get(self._STORAGE_ACCOUNT_KEY, None)
        Precondition.check_string_not_empty(
            self._account_name, "The Azure account name should not be empty"
        )
        Precondition.check_string_not_empty(
            self._account_key, "The Azure account key should not be empty"
        )
        Precondition.check_argument(
            expire_time_in_ms == 0,
            "The expiration time of Azure account key credential should be 0",
        )

    def credential_type(self) -> str:
        """Returns the type of the credential.

        Returns:
            The type of the credential.
        """
        return self.AZURE_ACCOUNT_KEY_CREDENTIAL_TYPE

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
            self._STORAGE_ACCOUNT_NAME: self._account_name,
            self._STORAGE_ACCOUNT_KEY: self._account_key,
        }

    def account_name(self) -> str:
        """The Azure account name.

        Returns:
            The Azure account name.
        """
        return self._account_name

    def account_key(self) -> str:
        """The Azure account key.

        Returns:
            The Azure account key.
        """
        return self._account_key
