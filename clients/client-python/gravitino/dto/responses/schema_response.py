"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.

"""

from dataclasses import dataclass, field

from dataclasses_json import DataClassJsonMixin, config

from gravitino.dto.responses.base_response import BaseResponse
from gravitino.dto.schema_dto import SchemaDTO


@dataclass
class SchemaResponse(BaseResponse, DataClassJsonMixin):
    """Represents a response for a schema."""

    _schema: SchemaDTO = field(metadata=config(field_name="schema"))

    # TODO
    # pylint: disable=arguments-differ
    def schema(self) -> SchemaDTO:
        return self._schema

    def validate(self):
        """Validates the response data.

        Raises:
            IllegalArgumentException if catalog identifiers are not set.
        """
        super().validate()

        assert self._schema is not None, "schema must be non-null"
        assert (
            self._schema.name() is not None
        ), "schema 'name' must not be null and empty"
        assert self._schema.audit_info() is not None, "schema 'audit' must not be null"
