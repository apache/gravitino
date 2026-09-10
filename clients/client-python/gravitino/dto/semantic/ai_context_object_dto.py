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

import copy
from typing import Any, Optional

from gravitino.api.semantic.ai_context_object import AIContextObject


class AIContextObjectDTO:
    """Represents a structured AI context DTO.

    Unknown properties are carried in `additional_properties` and are flattened
    next to the standard properties on the wire.
    """

    def __init__(
        self,
        instructions: Optional[str] = None,
        synonyms: Optional[list[str]] = None,
        examples: Optional[list[str]] = None,
        additional_properties: Optional[dict[str, Any]] = None,
    ):
        self._instructions = instructions
        self._synonyms = None if synonyms is None else list(synonyms)
        self._examples = None if examples is None else list(examples)
        self._additional_properties = (
            {}
            if additional_properties is None
            else copy.deepcopy(additional_properties)
        )

    def instructions(self) -> Optional[str]:
        """Returns the free-form instructions, or `None` if it is not set."""
        return self._instructions

    def synonyms(self) -> Optional[list[str]]:
        """Returns the synonyms, or `None` if they are not set."""
        return None if self._synonyms is None else list(self._synonyms)

    def examples(self) -> Optional[list[str]]:
        """Returns the examples, or `None` if they are not set."""
        return None if self._examples is None else list(self._examples)

    def additional_properties(self) -> dict[str, Any]:
        """Returns the additional properties, empty if none are set."""
        return copy.deepcopy(self._additional_properties)

    @staticmethod
    def from_ai_context_object(
        ai_context_object: AIContextObject,
    ) -> "AIContextObjectDTO":
        """Convert a structured AI context to its DTO."""
        return AIContextObjectDTO(
            instructions=ai_context_object.instructions(),
            synonyms=ai_context_object.synonyms(),
            examples=ai_context_object.examples(),
            additional_properties=ai_context_object.additional_properties(),
        )

    def to_ai_context_object(self) -> AIContextObject:
        """Convert this DTO to a structured AI context."""
        return AIContextObject(
            instructions=self._instructions,
            synonyms=self._synonyms,
            examples=self._examples,
            additional_properties=self._additional_properties,
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, AIContextObjectDTO):
            return False
        return (
            self._instructions == other.instructions()
            and self._synonyms == other.synonyms()
            and self._examples == other.examples()
            and self._additional_properties == other.additional_properties()
        )

    def __hash__(self) -> int:
        return hash(
            (
                self._instructions,
                None if self._synonyms is None else tuple(self._synonyms),
                None if self._examples is None else tuple(self._examples),
                tuple(sorted(self._additional_properties)),
            )
        )

    def __repr__(self) -> str:
        return (
            f"AIContextObjectDTO(instructions={self._instructions!r}, "
            f"synonyms={self._synonyms!r}, examples={self._examples!r}, "
            f"additionalProperties={self._additional_properties!r})"
        )
