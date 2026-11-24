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


"""Changes for the ontology and mapping alter operations."""

from __future__ import annotations

from copy import deepcopy
from typing import Optional


class OntologyChange:
    """An immutable update to an ontology."""

    def __init__(self, update: dict):
        self._update = deepcopy(update)

    @classmethod
    def rename(cls, new_name: str) -> OntologyChange:
        """Renames the object."""
        return cls({"@type": "rename", "newName": new_name})

    @classmethod
    def update_comment(cls, new_comment: Optional[str]) -> OntologyChange:
        """Updates or clears the object's comment."""
        return cls({"@type": "updateComment", "newComment": new_comment})

    @classmethod
    def set_property(cls, property_name: str, value: str) -> OntologyChange:
        """Sets a property to the supplied value."""
        return cls({"@type": "setProperty", "property": property_name, "value": value})

    @classmethod
    def remove_property(cls, property_name: str) -> OntologyChange:
        """Removes a property."""
        return cls({"@type": "removeProperty", "property": property_name})

    @classmethod
    def replace_definition(cls, definition: dict) -> OntologyChange:
        """Replaces the complete native definition."""
        return cls({"@type": "replaceDefinition", "definition": definition})

    def to_dict(self) -> dict:
        """Returns a copy of the update body."""
        return deepcopy(self._update)


class OntologyMappingChange(OntologyChange):
    """A mapping update. The referenced ontology is fixed for the mapping's lifetime."""
