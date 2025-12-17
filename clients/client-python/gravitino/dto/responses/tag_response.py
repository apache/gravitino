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

from dataclasses_json import config, dataclass_json

from gravitino.dto.responses.base_response import BaseResponse
from gravitino.dto.tag_dto import TagDTO
from gravitino.utils.precondition import Precondition


@dataclass_json
@dataclass
class TagNamesListResponse(BaseResponse):
    """Represents a response for a Tag Names List request."""

    _tags: list[str] = field(default_factory=list, metadata=config(field_name="names"))

    def tag_names(self) -> list[str]:
        return self._tags

    def validate(self) -> None:
        Precondition.check_argument(
            self._tags is not None, "Tag Names List response should have tags"
        )

        for tag_name in self._tags:
            Precondition.check_string_not_empty(
                tag_name, "Tag Names List response should have non-empty tag names"
            )


@dataclass_json
@dataclass
class TagListResponse(BaseResponse):
    """Represents a response for a Tag List request."""

    _tags: list[TagDTO] = field(
        default_factory=list, metadata=config(field_name="tags")
    )

    def tags(self) -> list[TagDTO]:
        return self._tags


@dataclass_json
@dataclass
class TagResponse(BaseResponse):
    """Represents a response for a tag."""

    _tag: TagDTO = field(default=None, metadata=config(field_name="tag"))

    def tag(self) -> TagDTO:
        return self._tag

    def validate(self) -> None:
        Precondition.check_argument(
            self._tag is not None, "Tag response should have a tag"
        )
