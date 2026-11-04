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
from abc import ABC, abstractmethod
from dataclasses import dataclass, field

from dataclasses_json import config, dataclass_json

from gravitino.api.policy.policy_change import PolicyChange
from gravitino.dto.policy_dto import PolicyContentDTO
from gravitino.rest.rest_message import RESTRequest
from gravitino.utils.precondition import Precondition


@dataclass
@dataclass_json
class PolicyUpdateRequestBase(RESTRequest, ABC):
    _type: str = field(init=False, metadata=config(field_name="@type"))

    @abstractmethod
    def policy_change(self) -> PolicyChange:
        """Returns the policy change this request represents."""
        raise NotImplementedError()


class PolicyUpdateRequest:
    """Request to update a policy."""

    @dataclass_json
    @dataclass
    class RenamePolicyRequest(PolicyUpdateRequestBase):
        """The policy update request for renaming a policy."""

        _new_name: str = field(init=True, metadata=config(field_name="newName"))

        def __post_init__(self) -> None:
            self._type = "rename"

        def validate(self) -> None:
            """Validates the request.

            Raises:
                IllegalArgumentException: If the request is invalid.
            """
            Precondition.check_string_not_empty(
                self._new_name,
                '"newName" must not be blank',
            )

        @property
        def new_name(self) -> str:
            return self._new_name

        def policy_change(self) -> PolicyChange.RenamePolicy:
            return PolicyChange.rename(self._new_name)

    @dataclass_json
    @dataclass
    class UpdatePolicyCommentRequest(PolicyUpdateRequestBase):
        """The policy update request for updating a policy comment."""

        _new_comment: str | None = field(metadata=config(field_name="newComment"))

        def __post_init__(self) -> None:
            self._type = "updateComment"

        def validate(self) -> None:
            """Validates the request."""
            # always pass

        @property
        def new_comment(self) -> str | None:
            return self._new_comment

        def policy_change(self) -> PolicyChange.UpdatePolicyComment:
            return PolicyChange.UpdatePolicyComment(self._new_comment)

    @dataclass
    class UpdatePolicyContentRequest(PolicyUpdateRequestBase):
        """The policy update request for updating a policy's content.

        Deserialization is implemented explicitly (rather than through
        dataclasses_json) because the concrete type of the ``newContent``
        field is selected by the sibling ``policyType`` field, which
        dataclasses_json cannot dispatch on. Serialization still goes
        through the inherited dataclasses_json machinery.
        """

        _policy_type: str = field(metadata=config(field_name="policyType"))
        _new_content: PolicyContentDTO = field(
            metadata=config(
                field_name="newContent",
                encoder=lambda content: (
                    content.to_dict() if content is not None else None
                ),
            )
        )

        def __post_init__(self) -> None:
            self._type = "updateContent"

        def validate(self) -> None:
            """Validates the request.

            Raises:
                IllegalArgumentException: If the request is invalid.
            """
            Precondition.check_string_not_empty(
                self._policy_type,
                '"policyType" must not be blank',
            )
            Precondition.check_argument(
                self._new_content is not None, '"newContent" must not be null'
            )
            self._new_content.validate()

        @property
        def policy_type(self) -> str:
            return self._policy_type

        @property
        def new_content(self) -> PolicyContentDTO:
            return self._new_content

        @classmethod
        def from_dict(
            cls, kvs: dict, *, infer_missing: bool = False
        ) -> PolicyUpdateRequest.UpdatePolicyContentRequest:
            """Deserializes an update-content request, dispatching the new
            content on the sibling ``policyType`` field, as
            PolicyDTO.from_dict does for a policy's content.

            Args:
                kvs: The serialized request.
                infer_missing: Accepted for compatibility with the
                    dataclasses_json API; content deserialization always
                    infers missing fields, mirroring PolicyContentDTO.

            Returns:
                UpdatePolicyContentRequest: The deserialized request.
            """
            content_data = kvs.get("newContent")
            return cls(
                _policy_type=kvs.get("policyType"),
                _new_content=(
                    PolicyContentDTO.from_dict_by_type(
                        content_data, kvs.get("policyType")
                    )
                    if content_data is not None
                    else None
                ),
            )

        @classmethod
        def from_json(
            cls, s: str, *, infer_missing: bool = False, **kwargs
        ) -> PolicyUpdateRequest.UpdatePolicyContentRequest:
            """Deserializes an update-content request from a JSON string.

            Extra keyword arguments are passed through to json.loads.
            """
            return cls.from_dict(json.loads(s, **kwargs), infer_missing=infer_missing)

        def policy_change(self) -> PolicyChange.UpdateContent:
            return PolicyChange.UpdateContent(
                self._policy_type, self._new_content.to_domain_content()
            )
