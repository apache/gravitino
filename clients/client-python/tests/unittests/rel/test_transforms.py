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

from gravitino.api.expressions.literals.literals import Literals
from gravitino.api.expressions.named_reference import NamedReference
from gravitino.api.expressions.transforms.transforms import Transforms


class TestTransforms(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._temporal_transforms = {
            Transforms.IdentityTransform: Transforms.identity,
            Transforms.YearTransform: Transforms.year,
            Transforms.MonthTransform: Transforms.month,
            Transforms.DayTransform: Transforms.day,
            Transforms.HourTransform: Transforms.hour,
        }

        cls._transform_names = {
            Transforms.IdentityTransform: Transforms.NAME_OF_IDENTITY,
            Transforms.YearTransform: Transforms.NAME_OF_YEAR,
            Transforms.MonthTransform: Transforms.NAME_OF_MONTH,
            Transforms.DayTransform: Transforms.NAME_OF_DAY,
            Transforms.HourTransform: Transforms.NAME_OF_HOUR,
        }

    def test_temporal_transforms(self):
        field_name = "dummy_field"
        for trans_cls, trans_func in self._temporal_transforms.items():
            trans_from_str = trans_func(field_name=field_name)
            trans_from_list = trans_func(field_name=[field_name])
            trans_dict = {trans_from_str: 1, trans_from_list: 2}

            self.assertIsInstance(trans_from_str, trans_cls)
            self.assertIsInstance(trans_from_list, trans_cls)

            self.assertEqual(trans_from_str.name(), self._transform_names[trans_cls])
            self.assertEqual(
                trans_from_str.arguments(), [NamedReference.field([field_name])]
            )

            self.assertEqual(trans_from_str, trans_from_list)
            self.assertFalse(trans_from_str == field_name)
            self.assertEqual(len(trans_dict), 1)
            self.assertEqual(trans_dict[trans_from_str], 2)

    def test_bucket_transform(self):
        field_names = [["dummy_field"], [f"dummy_field_{i}" for i in range(2)]]
        num_buckets = 10
        bucket_transform = Transforms.bucket(num_buckets, *field_names)
        twin_bucket_transform = Transforms.bucket(num_buckets, *field_names)
        bucket_trans_dict = {
            bucket_transform: 1,
            twin_bucket_transform: 2,
        }

        self.assertIsInstance(bucket_transform, Transforms.BucketTransform)
        self.assertEqual(bucket_transform.name(), Transforms.NAME_OF_BUCKET)
        self.assertEqual(bucket_transform.num_buckets(), num_buckets)
        self.assertListEqual(bucket_transform.field_names(), field_names)
        self.assertListEqual(
            bucket_transform.arguments(),
            [Literals.integer_literal(num_buckets), *bucket_transform.fields],
        )
        self.assertEqual(bucket_transform, bucket_transform)
        self.assertIsNot(bucket_transform, twin_bucket_transform)
        self.assertEqual(bucket_transform, twin_bucket_transform)
        self.assertEqual(len(bucket_trans_dict), 1)
        self.assertEqual(bucket_trans_dict[bucket_transform], 2)

    def test_truncate_transform(self):
        field_name = "dummy_field"
        width = 10
        truncate_transform_str = Transforms.truncate(width, field_name)
        truncate_transform_list = Transforms.truncate(width, [field_name])
        truncate_trans_dict = {
            truncate_transform_str: 1,
            truncate_transform_list: 2,
        }

        self.assertIsInstance(truncate_transform_str, Transforms.TruncateTransform)
        self.assertIsInstance(truncate_transform_list, Transforms.TruncateTransform)
        self.assertEqual(truncate_transform_str.name(), Transforms.NAME_OF_TRUNCATE)
        self.assertEqual(truncate_transform_str.width(), width)
        self.assertListEqual(truncate_transform_str.field_name(), [field_name])
        self.assertListEqual(
            truncate_transform_str.arguments(),
            [Literals.integer_literal(width), truncate_transform_str.field],
        )
        self.assertEqual(truncate_transform_str, truncate_transform_str)
        self.assertIsNot(truncate_transform_str, truncate_transform_list)
        self.assertEqual(truncate_transform_str, truncate_transform_list)
        self.assertEqual(len(truncate_trans_dict), 1)
        self.assertEqual(truncate_trans_dict[truncate_transform_str], 2)
