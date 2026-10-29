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

from gravitino.api.ontology.concept_type import ConceptType
from gravitino.api.ontology.multiplicity import Multiplicity
from gravitino.api.ontology.ontology import Ontology
from gravitino.api.ontology.ontology_change import OntologyChange
from gravitino.api.ontology.ontology_concept import OntologyConcept
from gravitino.api.ontology.ontology_definition import OntologyDefinition
from gravitino.api.ontology.ontology_operations import OntologyOperations
from gravitino.api.ontology.ontology_relationship import OntologyRelationship
from gravitino.api.ontology.ontology_role import OntologyRole

__all__ = [
    "ConceptType",
    "Multiplicity",
    "Ontology",
    "OntologyChange",
    "OntologyConcept",
    "OntologyDefinition",
    "OntologyOperations",
    "OntologyRelationship",
    "OntologyRole",
]
