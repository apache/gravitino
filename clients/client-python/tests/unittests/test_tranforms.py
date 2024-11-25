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
from gravitino.api.expressions.transforms import (
    Transforms,
    IdentityTransform,
    YearTransform,
    MonthTransform,
    DayTransform,
    HourTransform,
    BucketTransform,
    TruncateTransform,
    ListTransform,
    RangeTransform,
    ApplyTransform,
)
from gravitino.api.expressions.named_reference import NamedReference, FieldReference
from gravitino.api.expressions.literals import Literals


class TestTransforms(unittest.TestCase):

    def setUp(self):
        self.ref = NamedReference.field(["some_field"])

    def test_identity_transform(self):
        transform = Transforms.identity(["some_field"])
        self.assertIsInstance(transform, IdentityTransform)
        self.assertEqual(transform.name(), "identity")
        self.assertEqual(transform.arguments(), [self.ref])

    def test_year_transform(self):
        transform = Transforms.year(["some_field"])
        self.assertIsInstance(transform, YearTransform)
        self.assertEqual(transform.name(), "year")
        self.assertEqual(transform.arguments(), [self.ref])

    def test_month_transform(self):
        transform = Transforms.month(["some_field"])
        self.assertIsInstance(transform, MonthTransform)
        self.assertEqual(transform.name(), "month")
        self.assertEqual(transform.arguments(), [self.ref])

    def test_day_transform(self):
        transform = Transforms.day(["some_field"])
        self.assertIsInstance(transform, DayTransform)
        self.assertEqual(transform.name(), "day")
        self.assertEqual(transform.arguments(), [self.ref])

    def test_hour_transform(self):
        transform = Transforms.hour(["some_field"])
        self.assertIsInstance(transform, HourTransform)
        self.assertEqual(transform.name(), "hour")
        self.assertEqual(transform.arguments(), [self.ref])

    def test_bucket_transform(self):
        field1 = FieldReference(["field1"])
        field2 = FieldReference(["field2"])
        transform = BucketTransform(num_buckets=5, fields=[field1, field2])
        self.assertEqual(transform.num_buckets, 5)
        self.assertEqual(transform.field_names, ["field1", "field2"])
        self.assertEqual(transform.name(), "bucket")
        expected_arguments = [str(Literals.integer_literal(5))] + ["field1", "field2"]
        self.assertEqual(transform.arguments(), expected_arguments)

    def test_truncate_transform(self):
        transform = Transforms.truncate(5, "some_field")
        self.assertIsInstance(transform, TruncateTransform)
        self.assertEqual(transform.name(), "truncate")
        self.assertEqual(transform.arguments(), [Literals.integer_literal(5), self.ref])

    def test_list_transform(self):
        transform = Transforms.list([["field1"], ["field2"]])
        self.assertIsInstance(transform, ListTransform)
        self.assertEqual(transform.name(), "list")
        self.assertEqual(len(transform.arguments()), 2)  # Expecting 2 field names

    def test_range_transform(self):
        transform = Transforms.range(["some_field"])
        self.assertIsInstance(transform, RangeTransform)
        self.assertEqual(transform.name(), "range")
        self.assertEqual(transform.arguments(), [self.ref])

    def test_apply_transform(self):
        transform = Transforms.apply("my_function", self.ref)
        self.assertIsInstance(transform, ApplyTransform)
        self.assertEqual(transform.name(), "my_function")
        self.assertEqual(transform.arguments(), [self.ref])
