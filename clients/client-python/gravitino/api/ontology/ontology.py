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


"""Snapshots of Gravitino ontologies and mappings."""

from __future__ import annotations

from copy import deepcopy
from typing import Optional

from gravitino.api.audit import Audit
from gravitino.api.policy.supports_policies import SupportsPolicies
from gravitino.api.tag.supports_tags import SupportsTags
from gravitino.dto.audit_dto import AuditDTO


class _DefinitionObject:
    def __init__(self, payload: dict):
        self._name = payload["name"]
        self._comment = payload.get("comment")
        self._definition = deepcopy(payload["definition"])
        self._properties = dict(payload["properties"])
        self._audit = AuditDTO.from_dict(payload["audit"])

    def name(self) -> str:
        """Returns the object's current name."""
        return self._name

    def comment(self) -> Optional[str]:
        """Returns the optional description."""
        return self._comment

    def definition(self) -> dict:
        """Returns a copy of the native, camel-case definition."""
        return deepcopy(self._definition)

    def properties(self) -> dict[str, str]:
        """Returns a copy of the Gravitino properties."""
        return dict(self._properties)

    def audit_info(self) -> Audit:
        """Returns the current audit information."""
        return self._audit


class Ontology(_DefinitionObject):
    """A metalake-wide vocabulary of concepts and relationships."""

    def __init__(self, payload: dict, tags: SupportsTags, policies: SupportsPolicies):
        super().__init__(payload)
        self._tags = tags
        self._policies = policies

    def supports_tags(self) -> SupportsTags:
        """Returns the ontology's tag operations."""
        return self._tags

    def supports_policies(self) -> SupportsPolicies:
        """Returns the ontology's policy operations."""
        return self._policies


class OntologyMapping(_DefinitionObject):
    """A mapping that binds a semantic model to an ontology."""

    def __init__(self, payload: dict):
        super().__init__(payload)
        self._ontology = payload["ontology"]

    def ontology(self) -> str:
        """Returns the current name of the referenced ontology."""
        return self._ontology
