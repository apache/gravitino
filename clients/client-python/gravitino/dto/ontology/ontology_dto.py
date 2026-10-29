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

from dataclasses_json import config, dataclass_json

from gravitino.api.ontology.ontology import Ontology
from gravitino.dto.audit_dto import AuditDTO
from gravitino.dto.ontology.ontology_definition_dto import OntologyDefinitionDTO
from gravitino.dto.ontology.ontology_dto_utils import normalize_ontology_status
from gravitino.dto.semantic.semantic_dto_utils import is_none
from gravitino.utils.precondition import Precondition


@dataclass_json
@dataclass
class OntologyDTO(Ontology):
    """Represents an ontology Data Transfer Object (DTO)."""

    _name: Optional[str] = field(
        default=None, metadata=config(field_name="name", exclude=is_none)
    )
    _comment: Optional[str] = field(
        default=None, metadata=config(field_name="comment", exclude=is_none)
    )
    _properties: Optional[dict[str, str]] = field(
        default=None, metadata=config(field_name="properties", exclude=is_none)
    )
    _status: Optional[str] = field(
        default=None,
        metadata=config(
            field_name="status",
            encoder=normalize_ontology_status,
            decoder=normalize_ontology_status,
            exclude=is_none,
        ),
    )
    _definition: Optional[OntologyDefinitionDTO] = field(
        default=None, metadata=config(field_name="definition", exclude=is_none)
    )
    _audit: Optional[AuditDTO] = field(
        default=None, metadata=config(field_name="audit", exclude=is_none)
    )

    def name(self) -> str:
        """The name of the ontology.

        Returns:
            str: The ontology name.
        """
        return self._name

    def comment(self) -> Optional[str]:
        """The comment of the ontology.

        Returns:
            Optional[str]: The comment, or `None` if it is not set.
        """
        return self._comment

    def properties(self) -> dict[str, str]:
        """The Gravitino-specific properties of the ontology.

        Returns:
            dict[str, str]: The properties, empty if none are set.
        """
        return self._properties if self._properties is not None else {}

    def status(self) -> str:
        """The status of the ontology.

        Returns:
            str: One of `Ontology.Status.DRAFT` or `Ontology.Status.PUBLISHED`.
        """
        return self._status

    def definition(self) -> OntologyDefinitionDTO:
        """The immutable concept graph of the ontology.

        Returns:
            OntologyDefinitionDTO: The ontology definition.
        """
        return self._definition

    def audit_info(self) -> Optional[AuditDTO]:
        """The audit information of the ontology.

        Returns:
            Optional[AuditDTO]: The audit information.
        """
        return self._audit

    def validate(self) -> None:
        """Validate the ontology DTO.

        Raises:
            IllegalArgumentException: If the name or status is blank, or if the
                definition is missing or invalid.
        """
        Precondition.check_string_not_empty(self._name, '"name" must not be blank')
        Precondition.check_string_not_empty(self._status, '"status" must not be blank')
        Precondition.check_argument(
            self._definition is not None, '"definition" must be set'
        )
        self._definition.validate()
