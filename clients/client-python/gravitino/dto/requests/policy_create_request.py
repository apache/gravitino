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

import json
from dataclasses import dataclass, field

from dataclasses_json import config

from gravitino.dto.policy_dto import PolicyContentDTO
from gravitino.rest.rest_message import RESTRequest
from gravitino.utils.precondition import Precondition


@dataclass
class PolicyCreateRequest(RESTRequest):
    """Represents a request to create a policy.

    Deserialization is implemented explicitly (rather than through
    dataclasses_json) because the concrete type of the ``content`` field is
    selected by the sibling ``policyType`` field, which dataclasses_json
    cannot dispatch on. Serialization still goes through the inherited
    dataclasses_json machinery.
    """

    _name: str = field(metadata=config(field_name="name"))
    _policy_type: str = field(metadata=config(field_name="policyType"))
    _comment: str | None = field(default=None, metadata=config(field_name="comment"))
    _enabled: bool = field(default=True, metadata=config(field_name="enabled"))
    _content: PolicyContentDTO | None = field(
        default=None,
        metadata=config(
            field_name="content",
            encoder=lambda content: content.to_dict() if content is not None else None,
        ),
    )

    def validate(self) -> None:
        """Validates the request.

        Raises:
            IllegalArgumentException: If the request is invalid.
        """
        Precondition.check_string_not_empty(self._name, '"name" must not be blank')
        Precondition.check_string_not_empty(
            self._policy_type, '"policyType" must not be blank'
        )
        Precondition.check_argument(
            self._content is not None, '"content" must not be null'
        )
        self._content.validate()

    @classmethod
    def from_dict(
        cls, kvs: dict, *, infer_missing: bool = False
    ) -> PolicyCreateRequest:
        """Deserializes a policy create request, dispatching the content on
        the sibling ``policyType`` field, as PolicyDTO.from_dict does.

        Args:
            kvs: The serialized request.
            infer_missing: Accepted for compatibility with the
                dataclasses_json API; content deserialization always
                infers missing fields, mirroring PolicyContentDTO.

        Returns:
            PolicyCreateRequest: The deserialized request.
        """
        content_data = kvs.get("content")
        return cls(
            _name=kvs.get("name"),
            _policy_type=kvs.get("policyType"),
            _comment=kvs.get("comment"),
            _enabled=kvs.get("enabled", True),
            _content=(
                PolicyContentDTO.from_dict_by_type(content_data, kvs.get("policyType"))
                if content_data is not None
                else None
            ),
        )

    @classmethod
    def from_json(
        cls, s: str, *, infer_missing: bool = False, **kwargs
    ) -> PolicyCreateRequest:
        """Deserializes a policy create request from a JSON string.

        Extra keyword arguments are passed through to json.loads.
        """
        return cls.from_dict(json.loads(s, **kwargs), infer_missing=infer_missing)

    @property
    def name(self) -> str:
        return self._name

    @property
    def policy_type(self) -> str:
        return self._policy_type

    @property
    def comment(self) -> str | None:
        return self._comment

    @property
    def enabled(self) -> bool:
        return self._enabled

    @property
    def content(self) -> PolicyContentDTO | None:
        return self._content
