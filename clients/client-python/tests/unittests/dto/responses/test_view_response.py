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

import unittest

from gravitino.api.rel.dialects import Dialects
from gravitino.dto.audit_dto import AuditDTO
from gravitino.dto.rel.sql_representation_dto import SQLRepresentationDTO
from gravitino.dto.rel.view_dto import ViewDTO
from gravitino.dto.responses.view_response import ViewResponse
from gravitino.exceptions.base import IllegalArgumentException


class TestViewResponse(unittest.TestCase):
    @staticmethod
    def _view_dto() -> ViewDTO:
        return ViewDTO(
            _name="test_view",
            _representations=[
                SQLRepresentationDTO(_dialect=Dialects.TRINO, _sql="SELECT 1")
            ],
            _audit=AuditDTO("creator"),
        )

    def test_view_response(self):
        response = ViewResponse(_code=0, _view=self._view_dto())

        response.validate()
        deserialized = ViewResponse.from_json(response.to_json())

        self.assertEqual("test_view", deserialized.view().name())
        self.assertEqual("creator", deserialized.view().audit_info().creator())
        self.assertEqual(1, len(deserialized.view().representations()))

    def test_view_response_validate(self):
        with self.assertRaises(IllegalArgumentException):
            ViewResponse(_code=0, _view=None).validate()
