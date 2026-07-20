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
from gravitino.api.rel.sql_representation import SQLRepresentation


class TestSQLRepresentation(unittest.TestCase):
    def test_predefined_dialects(self):
        self.assertEqual("trino", Dialects.TRINO)
        self.assertEqual("spark", Dialects.SPARK)
        self.assertEqual("hive", Dialects.HIVE)
        self.assertEqual("flink", Dialects.FLINK)

    def test_sql_representation(self):
        representation = SQLRepresentation(Dialects.TRINO, "SELECT 1")

        self.assertEqual(Representation.TYPE_SQL, representation.type())
        self.assertEqual(Dialects.TRINO, representation.dialect())
        self.assertEqual("SELECT 1", representation.sql())

    def test_sql_representation_equal_and_hash(self):
        representation = SQLRepresentation(Dialects.TRINO, "SELECT 1")
        equal_representation = SQLRepresentation(Dialects.TRINO, "SELECT 1")

        self.assertEqual(representation, equal_representation)
        self.assertEqual(hash(representation), hash(equal_representation))
        self.assertNotEqual(
            representation, SQLRepresentation(Dialects.SPARK, "SELECT 1")
        )
        self.assertNotEqual(representation, "invalid_representation")

    def test_sql_representation_validation(self):
        with self.assertRaisesRegex(ValueError, "dialect must not be null or empty"):
            SQLRepresentation("", "SELECT 1")
        with self.assertRaisesRegex(ValueError, "sql must not be null or empty"):
            SQLRepresentation(Dialects.TRINO, "")
