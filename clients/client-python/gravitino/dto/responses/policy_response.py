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

from gravitino.dto.policy_dto import (
    PolicyAssociationSelectorDTO,
    PolicyDTO,
    PolicyForTagAssociationDTO,
    TagForPolicyAssociationDTO,
)
from gravitino.dto.responses.base_response import BaseResponse
from gravitino.dto.responses.name_list_response import NameListResponse
from gravitino.exceptions.base import IllegalArgumentException


def _encode_policy(policy: PolicyDTO | None) -> dict | None:
    return policy.to_dict() if policy is not None else None


def _decode_policy(data) -> PolicyDTO | None:
    return PolicyDTO.from_dict(data) if isinstance(data, dict) else data


def _encode_policies(policies: list | None) -> list | None:
    if policies is None:
        return None
    return [policy.to_dict() for policy in policies]


def _decode_policies(data) -> list | None:
    if data is None:
        return None
    return [
        PolicyDTO.from_dict(policy) if isinstance(policy, dict) else policy
        for policy in data
    ]


@dataclass_json
@dataclass
class PolicyResponse(BaseResponse):
    """Represents a response containing a policy."""

    _policy: PolicyDTO | None = field(
        default=None,
        metadata=config(
            field_name="policy", encoder=_encode_policy, decoder=_decode_policy
        ),
    )

    def policy(self) -> PolicyDTO:
        return self._policy

    def validate(self) -> None:
        super().validate()
        if self._policy is None:
            raise IllegalArgumentException("Policy must not be null in response")


@dataclass_json
@dataclass
class PolicyListResponse(BaseResponse):
    """Represents a response containing a list of policies."""

    _policies: list | None = field(
        default=None,
        metadata=config(
            field_name="policies",
            encoder=_encode_policies,
            decoder=_decode_policies,
        ),
    )

    def policies(self) -> list:
        return self._policies

    def validate(self) -> None:
        super().validate()
        if self._policies is None:
            raise IllegalArgumentException("Policies must not be null in response")
        for policy in self._policies:
            if policy.content() is None:
                raise IllegalArgumentException(
                    "Policy content must not be null in response"
                )
            policy.content().validate()


@dataclass_json
@dataclass
class PolicyNameListResponse(NameListResponse):
    """Represents a response containing a list of policy names, plus whether
    some policies are inaccessible to the caller."""

    _has_inaccessible_policies: bool = field(
        default=False, metadata=config(field_name="hasInaccessiblePolicies")
    )

    def has_inaccessible_policies(self) -> bool:
        return self._has_inaccessible_policies


@dataclass_json
@dataclass
class MetadataObjectPolicyListResponse(PolicyListResponse):
    """Represents a response containing the policies of a metadata object,
    plus whether some policies are inaccessible to the caller."""

    _has_inaccessible_policies: bool = field(
        default=False, metadata=config(field_name="hasInaccessiblePolicies")
    )

    def has_inaccessible_policies(self) -> bool:
        return self._has_inaccessible_policies


@dataclass_json
@dataclass
class PolicyTagAssociationResponse(BaseResponse):
    """Represents a response containing a policy-tag association."""

    _policy: str | None = field(default=None, metadata=config(field_name="policy"))
    _tag: str | None = field(default=None, metadata=config(field_name="tag"))
    _selector: PolicyAssociationSelectorDTO | None = field(
        default=None, metadata=config(field_name="selector")
    )

    def policy(self) -> str:
        return self._policy

    def tag(self) -> str:
        return self._tag

    def selector(self) -> PolicyAssociationSelectorDTO:
        return self._selector

    def validate(self) -> None:
        super().validate()
        if self._policy is None or self._policy.strip() == "":
            raise IllegalArgumentException("Policy must not be blank in response")
        if self._tag is None or self._tag.strip() == "":
            raise IllegalArgumentException("Tag must not be blank in response")
        if self._selector is None:
            raise IllegalArgumentException("Selector must not be null in response")
        self._selector.validate()


@dataclass_json
@dataclass
class PolicyForTagAssociationListResponse(BaseResponse):
    """Represents a response containing the policy associations of a tag."""

    _associations: list[PolicyForTagAssociationDTO] | None = field(
        default=None, metadata=config(field_name="associations")
    )

    def associations(self) -> list:
        return self._associations

    def validate(self) -> None:
        super().validate()
        if self._associations is None:
            raise IllegalArgumentException("Associations must not be null in response")


@dataclass_json
@dataclass
class TagForPolicyAssociationListResponse(BaseResponse):
    """Represents a response containing the tag associations of a policy."""

    _associations: list[TagForPolicyAssociationDTO] | None = field(
        default=None, metadata=config(field_name="associations")
    )

    def associations(self) -> list:
        return self._associations

    def validate(self) -> None:
        super().validate()
        if self._associations is None:
            raise IllegalArgumentException("Associations must not be null in response")
