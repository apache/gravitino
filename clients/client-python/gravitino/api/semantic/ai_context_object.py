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
import math
from typing import Any, Final, Optional

from gravitino.api.semantic.semantic_utils import check_no_none_elements
from gravitino.exceptions.base import IllegalArgumentException
from gravitino.utils.precondition import Precondition

MAX_ADDITIONAL_PROPERTY_NESTING_DEPTH: Final[int] = 100

_STANDARD_PROPERTIES: Final[frozenset] = frozenset(
    {"instructions", "synonyms", "examples"}
)


class AIContextObject:
    """The structured form of AI context attached to a Semantic Model member.

    Unknown JSON-compatible properties are exposed through
    :meth:`additional_properties` and are retained losslessly.
    """

    def __init__(
        self,
        instructions: Optional[str] = None,
        synonyms: Optional[list[str]] = None,
        examples: Optional[list[str]] = None,
        additional_properties: Optional[dict[str, Any]] = None,
    ):
        check_no_none_elements("synonyms", synonyms)
        check_no_none_elements("examples", examples)

        self._instructions = instructions
        self._synonyms = None if synonyms is None else list(synonyms)
        self._examples = None if examples is None else list(examples)
        self._additional_properties = _normalize_additional_properties(
            additional_properties
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
        """Returns the additional JSON-compatible properties, empty if none are set."""
        return copy.deepcopy(self._additional_properties)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, AIContextObject):
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
            f"AIContextObject(instructions={self._instructions!r}, "
            f"synonyms={self._synonyms!r}, examples={self._examples!r}, "
            f"additionalProperties={self._additional_properties!r})"
        )


def _normalize_additional_properties(
    properties: Optional[dict[str, Any]],
) -> dict[str, Any]:
    if properties is None:
        return {}

    normalized = {}
    for name, value in properties.items():
        Precondition.check_argument(
            isinstance(name, str), "additional property name must be a string"
        )
        Precondition.check_argument(
            name not in _STANDARD_PROPERTIES,
            f"additional property must not duplicate standard property: {name}",
        )
        normalized[name] = _normalize_json_value(value, name, set(), 0)
    return normalized


def _normalize_json_value(
    value: Any, path: str, visiting: set[int], container_depth: int
) -> Any:
    if value is None or isinstance(value, (str, bool)):
        return value

    if isinstance(value, (int, float)):
        Precondition.check_argument(
            not isinstance(value, float) or math.isfinite(value),
            f"Additional property {path} must contain a finite number",
        )
        return value

    if isinstance(value, dict):
        return _normalize_json_dict(value, path, visiting, container_depth + 1)

    if isinstance(value, (list, tuple)):
        return _normalize_json_list(value, path, visiting, container_depth + 1)

    raise IllegalArgumentException(
        f"Additional property {path} has non-JSON-compatible value type: "
        f"{type(value).__name__}"
    )


def _normalize_json_dict(
    value: dict, path: str, visiting: set[int], container_depth: int
) -> dict[str, Any]:
    _enter_container(value, path, visiting, container_depth)
    try:
        normalized = {}
        for key, item in value.items():
            Precondition.check_argument(
                isinstance(key, str),
                f"Additional property {path} contains a map key that is not a string",
            )
            normalized[key] = _normalize_json_value(
                item, f"{path}.{key}", visiting, container_depth
            )
        return normalized
    finally:
        visiting.discard(id(value))


def _normalize_json_list(
    value, path: str, visiting: set[int], container_depth: int
) -> list[Any]:
    _enter_container(value, path, visiting, container_depth)
    try:
        return [
            _normalize_json_value(item, f"{path}[{index}]", visiting, container_depth)
            for index, item in enumerate(value)
        ]
    finally:
        visiting.discard(id(value))


def _enter_container(
    value: Any, path: str, visiting: set[int], container_depth: int
) -> None:
    Precondition.check_argument(
        container_depth <= MAX_ADDITIONAL_PROPERTY_NESTING_DEPTH,
        f"Additional property {path} exceeds maximum nesting depth of "
        f"{MAX_ADDITIONAL_PROPERTY_NESTING_DEPTH}",
    )
    Precondition.check_argument(
        id(value) not in visiting,
        f"Additional property {path} contains a cyclic value",
    )
    visiting.add(id(value))
