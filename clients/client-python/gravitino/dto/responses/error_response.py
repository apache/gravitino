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
from gravitino.constants.error import ErrorConstants, EXCEPTION_MAPPING
from gravitino.exceptions.base import BadRequestException, UnknownError


@dataclass
class ErrorResponse(BaseResponse):
    """Represents an error response."""

    _type: str = field(metadata=config(field_name="type"))
    _message: str = field(metadata=config(field_name="message"))
    _stack: Optional[List[str]] = field(metadata=config(field_name="stack"))

    def type(self):
        return self._type

    def message(self):
        return self._message

    def stack(self):
        return self._stack

    def validate(self):
        super().validate()

        if self._type is None or not self._type.strip():
            raise BadRequestException("Type cannot be None or empty")

        if self._message is None or not self._message.strip():
            raise BadRequestException("Message cannot be None or empty")

    def __repr__(self) -> str:
        return (
            f"ErrorResponse(code={self._code}, type={self._type}, message={self._message})"
            + ("\n\t" + "\n\t".join(self._stack) if self._stack is not None else "")
        )

    def format_error_message(self) -> str:
        return (
            f"{self._message}\n" + "\n".join(self._stack)
            if self._stack is not None
            else ""
        )

    @classmethod
    def generate_error_response(
        cls, exception: Exception, message: str
    ) -> "ErrorResponse":
        for exception_type, error_code in EXCEPTION_MAPPING.items():
            if issubclass(exception, exception_type):
                return cls(error_code, exception.__name__, message, None)

        return cls(
            ErrorConstants.UNKNOWN_ERROR_CODE, UnknownError.__name__, message, None
        )
