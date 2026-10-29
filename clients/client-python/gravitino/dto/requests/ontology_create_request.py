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

from gravitino.dto.ontology.ontology_definition_dto import OntologyDefinitionDTO
from gravitino.rest.rest_message import RESTRequest
from gravitino.utils.precondition import Precondition


@dataclass_json
@dataclass
class OntologyCreateRequest(RESTRequest):
    """Represents a request to create an ontology."""

    _name: str = field(metadata=config(field_name="name"))
    _comment: Optional[str] = field(default=None, metadata=config(field_name="comment"))
    _properties: Optional[dict[str, str]] = field(
        default_factory=dict, metadata=config(field_name="properties")
    )
    _definition: Optional[OntologyDefinitionDTO] = field(
        default=None, metadata=config(field_name="definition")
    )

    def validate(self) -> None:
        """Validate the request.

        Raises:
            IllegalArgumentException: If the name is blank or the definition is
                missing or invalid.
        """
        Precondition.check_string_not_empty(self._name, '"name" must not be blank')
        Precondition.check_argument(
            self._definition is not None, '"definition" must not be null'
        )
        self._definition.validate()
