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
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from dataclasses_json import config, dataclass_json

from gravitino.rest.rest_message import RESTRequest
from gravitino.utils.precondition import Precondition
from gravitino.utils.string_utils import StringUtils


@dataclass_json
@dataclass
class TagValuePairRequest(RESTRequest):
    """Represents a tag assignment value pair request."""

    _name: str = field(metadata=config(field_name="name"))
    _value: Optional[str] = field(default=None, metadata=config(field_name="value"))

    @property
    def name(self) -> str:
        """Gets the tag name."""
        return self._name

    @property
    def value(self) -> Optional[str]:
        """Gets the tag assignment value."""
        return self._value

    def validate(self) -> None:
        """Validates the request."""
        Precondition.check_argument(
            StringUtils.is_not_blank(self._name),
            "Tag name must not be null or empty",
        )
        if self._value is not None:
            Precondition.check_argument(
                self._value.strip() != "",
                "Tag value must not be empty",
            )
            Precondition.check_argument(
                len(self._value) <= 256,
                "Tag value must not be longer than 256 characters",
            )


@dataclass_json
@dataclass
class TagsAssociateRequest(RESTRequest):
    """Represents a request to associate tags."""

    _tags_to_add: Optional[list[str]] = field(
        default=None, metadata=config(field_name="tagsToAdd")
    )
    _tags_to_remove: Optional[list[str]] = field(
        default=None, metadata=config(field_name="tagsToRemove")
    )

    @property
    def tags_to_add(self) -> Optional[list[str]]:
        """Gets the tags to add."""
        return self._tags_to_add

    @property
    def tags_to_remove(self) -> Optional[list[str]]:
        """Gets the tags to remove."""
        return self._tags_to_remove

    def validate(self) -> None:
        """Validates the request."""
        Precondition.check_argument(
            self._tags_to_add is not None or self._tags_to_remove is not None,
            "tagsToAdd and tagsToRemove cannot both be null",
        )

        self._validate_tag_names(self._tags_to_add, "tagsToAdd")
        self._validate_tag_names(self._tags_to_remove, "tagsToRemove")

    def _validate_tag_names(
        self, tag_names: Optional[list[str]], field_name: str
    ) -> None:
        if tag_names is None:
            return

        Precondition.check_argument(
            all(StringUtils.is_not_blank(tag_name) for tag_name in tag_names),
            f"{field_name} must not contain null or empty tag names",
        )


@dataclass_json
@dataclass
class TagValuesAssociateRequest(RESTRequest):
    """Represents a request to associate tag-value pairs."""

    _tags_to_add: Optional[list[TagValuePairRequest]] = field(
        default=None, metadata=config(field_name="tagsToAdd")
    )
    _tags_to_remove: Optional[list[TagValuePairRequest]] = field(
        default=None, metadata=config(field_name="tagsToRemove")
    )

    def __post_init__(self) -> None:
        self._tags_to_add = self._normalize_pairs(self._tags_to_add)
        self._tags_to_remove = self._normalize_pairs(self._tags_to_remove)

    @property
    def tags_to_add(self) -> Optional[list[TagValuePairRequest]]:
        """Gets the tags to add."""
        return self._tags_to_add

    @property
    def tags_to_remove(self) -> Optional[list[TagValuePairRequest]]:
        """Gets the tags to remove."""
        return self._tags_to_remove

    def validate(self) -> None:
        """Validates the request."""
        Precondition.check_argument(
            bool(self._tags_to_add) or bool(self._tags_to_remove),
            "tagsToAdd and tagsToRemove cannot both be null or empty",
        )

        self._validate_pairs(self._tags_to_add, "tagsToAdd")
        self._validate_pairs(self._tags_to_remove, "tagsToRemove")
        self._validate_no_intersection()

    def _normalize_pairs(
        self, pairs: list[str | dict[str, Optional[str]] | TagValuePairRequest] | None
    ) -> Optional[list[TagValuePairRequest]]:
        if pairs is None:
            return None

        normalized_pairs = []
        for pair in pairs:
            if isinstance(pair, TagValuePairRequest):
                normalized_pairs.append(pair)
            elif isinstance(pair, str):
                normalized_pairs.append(TagValuePairRequest(pair))
            elif isinstance(pair, dict):
                normalized_pairs.append(
                    TagValuePairRequest(pair.get("name"), pair.get("value"))
                )
            else:
                raise TypeError(f"Unsupported tag value pair type: {type(pair)}")

        return normalized_pairs

    def _validate_pairs(
        self, pairs: Optional[list[TagValuePairRequest]], field_name: str
    ) -> None:
        if pairs is None:
            return

        Precondition.check_argument(
            all(pair is not None for pair in pairs),
            f"{field_name} must not contain null tag value pairs",
        )
        for pair in pairs:
            pair.validate()

    def _validate_no_intersection(self) -> None:
        if not self._tags_to_add or not self._tags_to_remove:
            return

        tags_to_add = {(pair.name, pair.value) for pair in self._tags_to_add}
        tags_to_remove = {(pair.name, pair.value) for pair in self._tags_to_remove}

        Precondition.check_argument(
            not tags_to_add.intersection(tags_to_remove),
            "tagsToAdd and tagsToRemove must not contain the same tag-value pair",
        )
