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

from typing import List, Optional
from dataclasses import dataclass, field
from dataclasses_json import config

from gravitino.dto.responses.base_response import BaseResponse
from gravitino.constants.error import ErrorConstants
from gravitino.exceptions.rest_exception import RESTException

@dataclass
class ErrorResponse(BaseResponse):
    """Represents an error response."""

    _type: str = field(metadata=config(field_name="type"))
    _message: str = field(metadata=config(field_name="message"))
    _stack: Optional[List[str]] = field(metadata=config(field_name="stack"))

    def validate(self):
        super().validate()

        assert self._type is not None and len(self._type) != 0, "type cannot be None or empty"
        assert self._message is not None and len(self._message) != 0, "message cannot be None or empty"

    def __repr__(self) -> str:
        return f"ErrorResponse(code={self._code}, type={self._type}, message={self._message})" + ("\n\t" + "\n\t".join(self._stack) if self._stack is not None else "")

    @classmethod
    def rest_error(cls, message: str) -> "ErrorResponse":
        """Creates a new rest error instance of ErrorResponse.

        Args:
            message: The message of the error.

        Returns:
            The new instance.
        """
        return cls(ErrorConstants.REST_ERROR_CODE, RESTException.__name__, message, None)
    
    @classmethod
    def illegal_arguments(cls, message: str) -> "ErrorResponse":
        """Create a new illegal arguments error instance of ErrorResponse.

        Args:
            message: The message of the error.

        Returns:
            The new instance.
        """
        return cls(ErrorConstants.ILLEGAL_ARGUMENTS_CODE, .__name__, message, None)