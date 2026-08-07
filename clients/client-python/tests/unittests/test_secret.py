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

from gravitino.api.secret import SecretBinding, SecretReference


class TestSecretModels(unittest.TestCase):
    def test_secret_binding_repr_redacts_plaintext(self):
        binding = SecretBinding(provider="memory", plaintext="s3cr3t")
        self.assertNotIn("s3cr3t", repr(binding))
        self.assertIn("***", repr(binding))

    def test_secret_reference_allows_empty_attributes(self):
        reference = SecretReference(provider="vault")
        self.assertEqual(reference.attributes, {})
        reference = SecretReference(provider="vault", attributes={})
        self.assertEqual(reference.attributes, {})

    def test_secret_reference_rejects_none_attributes(self):
        with self.assertRaises(ValueError):
            SecretReference(provider="vault", attributes=None)


if __name__ == "__main__":
    unittest.main()
