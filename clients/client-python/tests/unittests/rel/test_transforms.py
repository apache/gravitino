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
    Transforms,
)


class TestTransforms(unittest.TestCase):
    def test_identity_transform(self):
        field_name = "dummy_field"
        ident_trans_str = Transforms.identity(field_name=field_name)
        ident_trans_list = Transforms.identity(field_name=[field_name])
        ident_dict = {ident_trans_str: 1, ident_trans_list: 2}

        self.assertIsInstance(ident_trans_str, IdentityTransform)
        self.assertIsInstance(ident_trans_list, IdentityTransform)

        self.assertEqual(ident_trans_str.name(), Transforms.NAME_OF_IDENTITY)
        self.assertEqual(
            ident_trans_str.arguments(), [NamedReference.field([field_name])]
        )

        self.assertEqual(ident_trans_str, ident_trans_list)
        self.assertEqual(len(ident_dict), 1)
        self.assertEqual(ident_dict[ident_trans_str], 2)
