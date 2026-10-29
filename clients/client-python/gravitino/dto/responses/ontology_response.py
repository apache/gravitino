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

from gravitino.dto.ontology.ontology_dto import OntologyDTO
from gravitino.dto.responses.base_response import BaseResponse
from gravitino.utils.precondition import Precondition


@dataclass_json
@dataclass
class OntologyNamesListResponse(BaseResponse):
    """Represents a response for an ontology names list request."""

    _names: list[str] = field(default_factory=list, metadata=config(field_name="names"))

    def names(self) -> list[str]:
        """The ontology names.

        Returns:
            list[str]: The ontology names.
        """
        return self._names

    def validate(self) -> None:
        """Validate the response.

        Raises:
            IllegalArgumentException: If the names are missing or blank.
        """
        Precondition.check_argument(
            self._names is not None,
            "Ontology Names List response should have ontology names",
        )
        for name in self._names:
            Precondition.check_string_not_empty(
                name,
                "Ontology Names List response should have non-empty ontology names",
            )


@dataclass_json
@dataclass
class OntologyListResponse(BaseResponse):
    """Represents a response for an ontology list request."""

    _ontologies: list[OntologyDTO] = field(
        default_factory=list, metadata=config(field_name="ontologies")
    )

    def ontologies(self) -> list[OntologyDTO]:
        """The ontologies.

        Returns:
            list[OntologyDTO]: The ontologies.
        """
        return self._ontologies

    def validate(self) -> None:
        """Validate the response.

        Raises:
            IllegalArgumentException: If an ontology is missing or invalid.
        """
        Precondition.check_argument(
            self._ontologies is not None, '"ontologies" must not be null'
        )
        for ontology in self._ontologies:
            Precondition.check_argument(
                ontology is not None, "ontologies must not contain null"
            )
            ontology.validate()


@dataclass_json
@dataclass
class OntologyResponse(BaseResponse):
    """Represents a response for an ontology."""

    _ontology: Optional[OntologyDTO] = field(
        default=None, metadata=config(field_name="ontology")
    )

    def ontology(self) -> OntologyDTO:
        """The ontology.

        Returns:
            OntologyDTO: The ontology.
        """
        return self._ontology

    def validate(self) -> None:
        """Validate the response.

        Raises:
            IllegalArgumentException: If the ontology is missing or invalid.
        """
        Precondition.check_argument(
            self._ontology is not None, "Ontology response should have an ontology"
        )
        self._ontology.validate()
