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

from typing import Any, Optional, Union

from gravitino.dto.semantic.ai_context_dto import AIContextDTO
from gravitino.dto.semantic.ai_context_object_dto import AIContextObjectDTO
from gravitino.utils.precondition import Precondition

_INSTRUCTIONS = "instructions"
_SYNONYMS = "synonyms"
_EXAMPLES = "examples"


class AIContextSerdes:
    """Serdes for AI context DTOs.

    An AI context is either a bare JSON string or a JSON object whose unknown
    properties are retained losslessly.
    """

    @staticmethod
    def serialize(
        value: Optional[AIContextDTO],
    ) -> Optional[Union[str, dict[str, Any]]]:
        """Encode an AI context DTO to a string or a dictionary."""
        if value is None:
            return None
        if value.text() is not None:
            return value.text()
        return _serialize_object(value.object())

    @staticmethod
    def deserialize(
        value: Optional[Union[str, dict[str, Any]]],
    ) -> Optional[AIContextDTO]:
        """Decode an AI context DTO from a string or a dictionary."""
        if value is None:
            return None
        if isinstance(value, str):
            return AIContextDTO(text=value)
        Precondition.check_argument(
            isinstance(value, dict), "AI context must be a string or object"
        )
        return AIContextDTO(obj=_deserialize_object(value))


def _serialize_object(value: AIContextObjectDTO) -> dict[str, Any]:
    serialized: dict[str, Any] = {}
    if value.instructions() is not None:
        serialized[_INSTRUCTIONS] = value.instructions()
    if value.synonyms() is not None:
        serialized[_SYNONYMS] = value.synonyms()
    if value.examples() is not None:
        serialized[_EXAMPLES] = value.examples()
    serialized.update(value.additional_properties())
    return serialized


def _deserialize_object(value: dict[str, Any]) -> AIContextObjectDTO:
    additional_properties = {
        name: item
        for name, item in value.items()
        if name not in (_INSTRUCTIONS, _SYNONYMS, _EXAMPLES)
    }
    return AIContextObjectDTO(
        instructions=_read_string(value, _INSTRUCTIONS),
        synonyms=_read_string_list(value, _SYNONYMS),
        examples=_read_string_list(value, _EXAMPLES),
        additional_properties=additional_properties,
    )


def _read_string(value: dict[str, Any], name: str) -> Optional[str]:
    if name not in value:
        return None
    item = value[name]
    Precondition.check_argument(isinstance(item, str), f"{name} must be a string")
    return item


def _read_string_list(value: dict[str, Any], name: str) -> Optional[list[str]]:
    if name not in value:
        return None
    items = value[name]
    Precondition.check_argument(
        isinstance(items, list) and all(isinstance(item, str) for item in items),
        f"{name} must be an array of strings",
    )
    return list(items)
