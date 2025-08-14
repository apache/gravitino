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

from typing import TypeVar

from gravitino.constants.timeout import TIMEOUT

T = TypeVar("T")


class GravitinoClientConfig:
    """
    Configuration class for Gravitino Python client;
    It encapsulates HTTP connection configurations and validates client properties.
    """

    POSITIVE_NUMBER_ERROR_MSG: str = "The value must be a positive number"
    """Error message for positive number validation"""

    GRAVITINO_CLIENT_CONFIG_PREFIX: str = "gravitino_client_"
    """Configuration key prefix for Gravitino client config"""

    CLIENT_REQUEST_TIMEOUT_DEFAULT: int = TIMEOUT
    """Default HTTP request timeout in seconds"""

    CLIENT_REQUEST_TIMEOUT: str = "gravitino_client_request_timeout"
    """Configuration key for request timeout"""

    SUPPORT_CLIENT_CONFIG_KEYS: set = {CLIENT_REQUEST_TIMEOUT}
    """Set of supported configuration keys"""

    def __init__(self, properties: dict):
        """Initializes the configuration with validated properties

        Args:
            properties: Key-value pairs of configuration parameters
        """
        self._properties = properties

    @classmethod
    def build_from_properties(cls, properties: dict) -> "GravitinoClientConfig":
        """Factory method to create configuration from properties

        Args:
            properties: Input configuration dictionary

        Returns:
            GravitinoClientConfig instance

        Raises:
            ValueError: If unsupported keys are provided
        """
        if properties is None:
            return cls({})
        for key in properties:
            if key not in cls.SUPPORT_CLIENT_CONFIG_KEYS:
                raise ValueError(f"Invalid property for client: {key}")
        return cls({key: properties[key] for key in properties})

    def get_client_request_timeout(self) -> int:
        """Retrieves and validates HTTP request timeout

        Returns:
            Timeout value in seconds

        Raises:
            IllegalArgumentException: If value is negative or value cannot be converted to integer
        """
        timeout = self._property_as_int(
            self.CLIENT_REQUEST_TIMEOUT, self.CLIENT_REQUEST_TIMEOUT_DEFAULT
        )
        self._check_value(
            self.CLIENT_REQUEST_TIMEOUT,
            timeout,
            lambda x: x >= 0,
            self.POSITIVE_NUMBER_ERROR_MSG,
        )
        return timeout

    def _property_as_int(self, key: str, default: int) -> int:
        """Safely converts property value to integer

        Args:
            key: Configuration key to retrieve
            default: Default value if key not found

        Returns:
            Converted integer value

        Raises:
            ValueError: If value cannot be converted to integer
        """
        value = self._properties.get(key)
        if value is None:
            return default
        try:
            return int(value)
        except (TypeError, ValueError) as err:
            raise ValueError(
                f"Value '{value}' for key '{key}' must be an integer"
            ) from err

    def _check_value(self, key: str, value: T, validator: callable, error_msg: str):
        """Generic validation method for configuration values

        Args:
            key: Config key
            value: Value to validate
            validator: Validation function returning boolean
            error_msg: Error message template

        Raises:
            ValueError: If validation fails
        """
        if not validator(value):
            raise ValueError(f"Value '{value}' for key '{key}' is invalid. {error_msg}")
