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

from typing import Optional, Union

from gravitino.api.semantic.ai_context_object import AIContextObject
from gravitino.exceptions.base import IllegalArgumentException
from gravitino.utils.precondition import Precondition


class AIContext:
    """An immutable wrapper holding exactly one of a free-form string or an
    :class:`AIContextObject`.

    Instances are created through :meth:`of`.
    """

    def __init__(self, text: Optional[str], obj: Optional[AIContextObject]):
        Precondition.check_argument(
            (text is None) != (obj is None),
            "AI context must contain exactly one of text or object",
        )
        self._text = text
        self._object = obj

    @staticmethod
    def of(value: Union[str, AIContextObject]) -> "AIContext":
        """Create an AI context from a string or a structured object.

        Args:
            value (str | AIContextObject): The AI context value.

        Returns:
            AIContext: The AI context holding the given value.

        Raises:
            IllegalArgumentException: If the value is `None` or an unsupported type.
        """
        if isinstance(value, str):
            return AIContext(value, None)
        if isinstance(value, AIContextObject):
            return AIContext(None, value)
        raise IllegalArgumentException(
            "AI context must be a string or an AIContextObject"
        )

    def is_text(self) -> bool:
        """Returns `True` if this AI context holds a string."""
        return self._text is not None

    def text(self) -> Optional[str]:
        """Returns the string value, or `None` if this holds a structured object."""
        return self._text

    def object(self) -> Optional[AIContextObject]:
        """Returns the structured value, or `None` if this holds a string."""
        return self._object

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, AIContext):
            return False
        return self._text == other.text() and self._object == other.object()

    def __hash__(self) -> int:
        return hash((self._text, self._object))

    def __repr__(self) -> str:
        return f"AIContext(text={self._text!r}, object={self._object!r})"
