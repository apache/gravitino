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

from typing import Optional

from gravitino.api.ontology.concept_type import ConceptType
from gravitino.api.ontology.multiplicity import Multiplicity
from gravitino.api.ontology.ontology import Ontology


def normalize_closed_set_value(
    value: Optional[str], allowed: tuple[str, ...]
) -> Optional[str]:
    """Return the canonical spelling of a closed-set value.

    The REST layer serializes closed sets in lower snake case (`entity_type`),
    while the documented and Python-facing values are upper snake case
    (`ENTITY_TYPE`). Matching ignores case and underscores, so both spellings
    decode to the canonical one and either spelling is accepted on write.

    Args:
        value (Optional[str]): The value to normalize, `None` is allowed.
        allowed (tuple[str, ...]): The closed set of canonical values.

    Returns:
        Optional[str]: The canonical value, the unchanged value if it is not in
            the set, or `None` if the input is `None`.
    """
    if not isinstance(value, str):
        return value
    normalized = value.replace("_", "").lower()
    for candidate in allowed:
        if candidate.replace("_", "").lower() == normalized:
            return candidate
    return value


def normalize_ontology_status(value: Optional[str]) -> Optional[str]:
    """Normalize an ontology status to `Ontology.Status` spelling.

    Args:
        value (Optional[str]): The status to normalize, `None` is allowed.

    Returns:
        Optional[str]: The canonical status, or `None` if the input is `None`.
    """
    return normalize_closed_set_value(value, Ontology.Status.ALL)


def normalize_concept_type(value: Optional[str]) -> Optional[str]:
    """Normalize a concept type to `ConceptType` spelling.

    Args:
        value (Optional[str]): The concept type to normalize, `None` is allowed.

    Returns:
        Optional[str]: The canonical concept type, or `None` if the input is
            `None`.
    """
    return normalize_closed_set_value(value, ConceptType.ALL)


def normalize_multiplicity(value: Optional[str]) -> Optional[str]:
    """Normalize a relationship multiplicity to `Multiplicity` spelling.

    Args:
        value (Optional[str]): The multiplicity to normalize, `None` is allowed.

    Returns:
        Optional[str]: The canonical multiplicity, or `None` if the input is
            `None`.
    """
    return normalize_closed_set_value(value, Multiplicity.ALL)
