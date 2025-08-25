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

from gravitino.api.expressions.indexes.index import Index
from gravitino.api.expressions.indexes.indexes import Indexes


class TestIndexes(unittest.TestCase):
    def test_indexes_class_vars(self):
        self.assertEqual(Indexes.EMPTY_INDEXES, [])
        self.assertEqual(Indexes.DEFAULT_MYSQL_PRIMARY_KEY_NAME, "PRIMARY")

    def test_indexes_create_index(self):
        field_names = [["col_1"], ["col_2"]]
        unique = Indexes.unique(name="unique", field_names=field_names)
        primary = Indexes.primary(name="primary", field_names=field_names)
        mysql_primary = Indexes.create_mysql_primary_key(field_names=field_names)

        self.assertIs(unique.type(), Index.IndexType.UNIQUE_KEY)
        self.assertIs(primary.type(), Index.IndexType.PRIMARY_KEY)
        self.assertIs(mysql_primary.type(), Index.IndexType.PRIMARY_KEY)

        self.assertEqual(unique.name(), "unique")
        self.assertEqual(primary.name(), "primary")
        self.assertEqual(mysql_primary.name(), "PRIMARY")

        self.assertListEqual(unique.field_names(), field_names)
        self.assertListEqual(primary.field_names(), field_names)
        self.assertListEqual(mysql_primary.field_names(), field_names)
