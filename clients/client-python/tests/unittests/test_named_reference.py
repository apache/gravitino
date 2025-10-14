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

from gravitino.api.rel.expressions.named_reference import FieldReference, NamedReference


class TestNamedReference(unittest.TestCase):
    def test_field_reference_creation(self):
        field = FieldReference(["student", "name"])
        self.assertEqual(field.field_name(), ["student", "name"])
        self.assertEqual(str(field), "student.name")

    def test_field_reference_equality(self):
        field1 = FieldReference(["student", "name"])
        field2 = FieldReference(["student", "name"])
        self.assertEqual(field1, field2)
        self.assertEqual(hash(field1), hash(field2))

    def test_named_reference_static_methods(self):
        ref = NamedReference.field(["student", "name"])
        self.assertEqual(ref.field_name(), ["student", "name"])

        ref2 = NamedReference.field_from_column("student")
        self.assertEqual(ref2.field_name(), ["student"])
