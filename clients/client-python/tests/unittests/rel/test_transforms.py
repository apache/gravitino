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

from gravitino.api.expressions.named_reference import NamedReference
from gravitino.api.expressions.transforms.transforms import (
    IdentityTransform,
    MonthTransform,
    Transforms,
    YearTransform,
)


class TestTransforms(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._temporal_transforms = {
            IdentityTransform: Transforms.identity,
            YearTransform: Transforms.year,
            MonthTransform: Transforms.month,
        }

        cls._transform_names = {
            IdentityTransform: Transforms.NAME_OF_IDENTITY,
            YearTransform: Transforms.NAME_OF_YEAR,
            MonthTransform: Transforms.NAME_OF_MONTH,
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
