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

from dataclasses import dataclass, field
from typing import Optional

from dataclasses_json import config

from gravitino.dto.requests.semantic_model_update_request import (
    SemanticModelUpdateRequest,
    SemanticModelUpdateRequestBase,
)
from gravitino.dto.semantic.json_serdes.semantic_json_mixin import SemanticJsonMixin
from gravitino.dto.semantic.semantic_dto_utils import is_none
from gravitino.rest.rest_message import RESTRequest
from gravitino.utils.precondition import Precondition


@dataclass
class SemanticModelUpdatesRequest(SemanticJsonMixin, RESTRequest):
    """Represents an atomic request to alter a Semantic Model."""

    _updates: list[SemanticModelUpdateRequestBase] = field(
        metadata=config(field_name="updates")
    )

    _expected_revision: Optional[int] = field(
        default=None, metadata=config(field_name="expectedRevision", exclude=is_none)
    )

    def validate(self):
        Precondition.check_argument(self._updates is not None, "updates cannot be null")
        Precondition.check_argument(len(self._updates) > 0, "updates cannot be empty")
        Precondition.check_argument(
            self._expected_revision is None
            or (
                isinstance(self._expected_revision, int)
                and not isinstance(self._expected_revision, bool)
                and self._expected_revision > 0
            ),
            "expected_revision must be a positive integer",
        )
        for update in self._updates:
            Precondition.check_argument(update is not None, "update cannot be null")
            Precondition.check_argument(
                not isinstance(
                    update,
                    SemanticModelUpdateRequest.ReplaceSemanticModelDefinitionRequest,
                )
                or self._expected_revision is not None,
                "expected_revision is required when replacing the definition",
            )
            Precondition.check_argument(
                not isinstance(
                    update, SemanticModelUpdateRequest.RenameSemanticModelRequest
                )
                or len(self._updates) == 1,
                "Rename must be the only Semantic Model change",
            )
            update.validate()
