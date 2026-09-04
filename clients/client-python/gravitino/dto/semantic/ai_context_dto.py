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

from typing import Optional

from gravitino.api.semantic.ai_context import AIContext
from gravitino.dto.semantic.ai_context_object_dto import AIContextObjectDTO
from gravitino.utils.precondition import Precondition


class AIContextDTO:
    """Represents an AI context DTO holding exactly one of a string or a
    structured object.

    A string AI context is serialized as a bare JSON string, a structured AI
    context is serialized as a JSON object.
    """

    def __init__(
        self,
        text: Optional[str] = None,
        obj: Optional[AIContextObjectDTO] = None,
    ):
        Precondition.check_argument(
            (text is None) != (obj is None),
            "AI context must contain exactly one of text or object",
        )
        self._text = text
        self._object = obj

    def text(self) -> Optional[str]:
        """Returns the string value, or `None` if this holds a structured object."""
        return self._text

    def object(self) -> Optional[AIContextObjectDTO]:
        """Returns the structured value, or `None` if this holds a string."""
        return self._object

    @staticmethod
    def from_ai_context(ai_context: AIContext) -> "AIContextDTO":
        """Convert an AI context to its DTO."""
        if ai_context.is_text():
            return AIContextDTO(text=ai_context.text())
        return AIContextDTO(
            obj=AIContextObjectDTO.from_ai_context_object(ai_context.object())
        )

    def to_ai_context(self) -> AIContext:
        """Convert this DTO to an AI context."""
        if self._text is not None:
            return AIContext.of(self._text)
        return AIContext.of(self._object.to_ai_context_object())

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, AIContextDTO):
            return False
        return self._text == other.text() and self._object == other.object()

    def __hash__(self) -> int:
        return hash((self._text, self._object))

    def __repr__(self) -> str:
        return f"AIContextDTO(text={self._text!r}, object={self._object!r})"
