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
from gravitino.api.rel.representation import Representation
from gravitino.dto.rel.sql_representation_dto import SQLRepresentationDTO


class TestSQLRepresentationDTO(unittest.TestCase):
    def test_sql_representation_dto(self):
        representation = SQLRepresentationDTO(
            _dialect=Dialects.TRINO, _sql="SELECT id FROM table"
        )

        self.assertEqual(Representation.TYPE_SQL, representation.type())
        self.assertEqual(Dialects.TRINO, representation.dialect())
        self.assertEqual("SELECT id FROM table", representation.sql())
        self.assertEqual(
            representation,
            SQLRepresentationDTO(_dialect=Dialects.TRINO, _sql="SELECT id FROM table"),
        )
        self.assertEqual(
            hash(representation),
            hash(
                SQLRepresentationDTO(
                    _dialect=Dialects.TRINO, _sql="SELECT id FROM table"
                )
            ),
        )
        self.assertNotEqual(
            representation,
            SQLRepresentationDTO(_dialect=Dialects.SPARK, _sql="SELECT id FROM table"),
        )

    def test_sql_representation_dto_validate(self):
        with self.assertRaises(ValueError):
            SQLRepresentationDTO(_dialect="", _sql="SELECT 1").validate()
        with self.assertRaises(ValueError):
            SQLRepresentationDTO(_dialect=Dialects.TRINO, _sql="").validate()
