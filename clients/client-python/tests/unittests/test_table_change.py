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

from gravitino.api.table_change import TableChange


class TestTableChange(unittest.TestCase):
    def test_rename(self):
        rename1, rename2 = (
            TableChange.rename(f"New table name {i + 1}") for i in range(2)
        )
        self.assertEqual(rename1.get_new_name(), "New table name 1")
        self.assertEqual(str(rename1), f"RENAMETABLE {rename1.get_new_name()}")
        self.assertFalse(rename1 == rename2)
        self.assertFalse(rename1 == "invalid_rename")
        self.assertTrue(rename1 == TableChange.rename("New table name 1"))
