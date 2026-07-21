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
from gravitino.client.generic_view import GenericView
from gravitino.dto.audit_dto import AuditDTO
from gravitino.dto.rel.sql_representation_dto import SQLRepresentationDTO
from gravitino.dto.rel.view_dto import ViewDTO


class TestGenericView(unittest.TestCase):
    def _generic_view(
        self,
        name: str = "demo_view",
        comment: str = "comment",
        sql: str = "SELECT 1",
        default_catalog: str = "demo_catalog",
        default_schema: str = "demo_schema",
    ) -> GenericView:
        return GenericView(
            ViewDTO(
                _name=name,
                _representations=[
                    SQLRepresentationDTO(_dialect=Dialects.TRINO, _sql=sql)
                ],
                _comment=comment,
                _default_catalog=default_catalog,
                _default_schema=default_schema,
                _properties={"key": "value"},
                _audit=AuditDTO("creator"),
            ),
        )

    def test_generic_view(self) -> None:
        generic_view = self._generic_view()

        self.assertEqual("demo_view", generic_view.name())
        self.assertEqual("comment", generic_view.comment())
        self.assertEqual("demo_catalog", generic_view.default_catalog())
        self.assertEqual("demo_schema", generic_view.default_schema())
        self.assertEqual({"key": "value"}, generic_view.properties())
        self.assertEqual(0, len(generic_view.columns()))
        self.assertEqual(1, len(generic_view.representations()))
        self.assertEqual("SELECT 1", generic_view.sql_for(Dialects.TRINO).sql())
        self.assertEqual("creator", generic_view.audit_info().creator())
