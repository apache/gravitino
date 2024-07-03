"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
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
