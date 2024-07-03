"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
"""

from abc import ABC, abstractmethod


class AuthDataProvider(ABC):
    """The provider of authentication data"""

    @abstractmethod
    def has_token_data(self) -> bool:
        """Judge whether AuthDataProvider can provide token data.

        Returns:
            true if the AuthDataProvider can provide token data otherwise false.
        """
        pass

    @abstractmethod
    def get_token_data(self) -> bytes:
        """Acquire the data of token for authentication. The client will set the token data as HTTP header
        Authorization directly. So the return value should ensure token data contain the token header
        (eg: Bearer, Basic) if necessary.

        Returns:
            the token data is used for authentication.
        """

    @abstractmethod
    def close(self):
        """Close the resource in the provider."""
