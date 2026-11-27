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


"""Maps ontology and mapping errors to Python client exceptions."""

from gravitino.dto.responses.error_response import ErrorResponse
from gravitino.exceptions.base import (
    IllegalOntologyException,
    IllegalOntologyMappingException,
    NoSuchOntologyException,
    NoSuchOntologyMappingException,
    OntologyAlreadyExistsException,
    OntologyInUseException,
    OntologyMappingAlreadyExistsException,
)
from gravitino.exceptions.handlers.semantic_model_error_handler import (
    SemanticModelErrorHandler,
)


class OntologyErrorHandler(SemanticModelErrorHandler):
    """Handles ontology, mapping, and parent semantic model errors."""

    _TYPES = {
        error.__name__: error
        for error in (
            IllegalOntologyException,
            IllegalOntologyMappingException,
            NoSuchOntologyException,
            NoSuchOntologyMappingException,
            OntologyAlreadyExistsException,
            OntologyMappingAlreadyExistsException,
            OntologyInUseException,
        )
    }

    def handle(self, error_response: ErrorResponse):
        error = self._TYPES.get(error_response.type())
        if error is not None:
            raise error(error_response.format_error_message())
        super().handle(error_response)


ONTOLOGY_ERROR_HANDLER = OntologyErrorHandler()
