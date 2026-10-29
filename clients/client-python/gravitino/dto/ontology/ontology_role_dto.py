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

from gravitino.api.ontology.ontology_role import OntologyRole
from gravitino.dto.semantic.semantic_dto_utils import is_none
from gravitino.utils.precondition import Precondition


@dataclass_json
@dataclass
class OntologyRoleDTO(OntologyRole):
    """Represents one role of an ontology relationship."""

    _concept: Optional[str] = field(
        default=None, metadata=config(field_name="concept", exclude=is_none)
    )
    _name: Optional[str] = field(
        default=None, metadata=config(field_name="name", exclude=is_none)
    )

    def concept(self) -> str:
        """The name of the concept playing this role.

        Returns:
            str: The concept name.
        """
        return self._concept

    def name(self) -> Optional[str]:
        """The optional role name.

        Returns:
            Optional[str]: The role name, or `None` if the concept alone
                identifies the role.
        """
        return self._name

    def validate(self) -> None:
        """Validate the role DTO.

        Raises:
            IllegalArgumentException: If the concept is blank.
        """
        Precondition.check_string_not_empty(
            self._concept, '"concept" must not be blank'
        )
