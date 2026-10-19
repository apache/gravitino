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

from scripts.run_lance_ray_matrix import (
    DEFAULT_VERSIONS,
    parse_version_specs,
)

FALLBACK = "lance-namespace==0.7.5"


class TestLanceRayMatrixVersions(unittest.TestCase):
    """Covers --versions parsing for the lance-ray compatibility matrix."""

    def test_paired_entry_pins_its_own_lance_namespace(self):
        specs = parse_version_specs("0.5.0=0.8.6", FALLBACK)

        self.assertEqual(1, len(specs))
        self.assertEqual("0.5.0", specs[0].version)
        self.assertEqual("lance-namespace==0.8.6", specs[0].lance_namespace_spec)

    def test_bare_entry_falls_back_to_default_spec(self):
        specs = parse_version_specs("0.4.2", FALLBACK)

        self.assertEqual("0.4.2", specs[0].version)
        self.assertEqual(FALLBACK, specs[0].lance_namespace_spec)

    def test_mixed_entries_keep_order_and_per_row_pins(self):
        specs = parse_version_specs("0.5.0=0.8.6, 0.4.2 ,0.2.0=0.4.5", FALLBACK)

        self.assertEqual(
            [
                ("0.5.0", "lance-namespace==0.8.6"),
                ("0.4.2", FALLBACK),
                ("0.2.0", "lance-namespace==0.4.5"),
            ],
            [(s.version, s.lance_namespace_spec) for s in specs],
        )

    def test_blank_entries_are_ignored(self):
        specs = parse_version_specs(" , 0.4.2 , ,", FALLBACK)

        self.assertEqual(["0.4.2"], [s.version for s in specs])

    def test_empty_value_yields_no_specs(self):
        self.assertEqual([], parse_version_specs("", FALLBACK))
        self.assertEqual([], parse_version_specs("  ,  ", FALLBACK))

    def test_missing_lance_namespace_version_is_rejected(self):
        with self.assertRaises(ValueError) as ctx:
            parse_version_specs("0.5.0=", FALLBACK)

        self.assertIn("missing lance-namespace version", str(ctx.exception))

    def test_missing_lance_ray_version_is_rejected(self):
        with self.assertRaises(ValueError) as ctx:
            parse_version_specs("=0.8.6", FALLBACK)

        self.assertIn("missing lance-ray version", str(ctx.exception))

    def test_duplicate_lance_ray_version_is_rejected(self):
        with self.assertRaises(ValueError) as ctx:
            parse_version_specs("0.4.2=0.7.5,0.4.2=0.8.6", FALLBACK)

        self.assertIn("duplicate lance-ray version", str(ctx.exception))

    def test_default_versions_parse_to_the_verified_pairings(self):
        # These pairings are the ones measured against a lance-namespace-core
        # 0.11.1 server; see DEFAULT_VERSIONS for why each one is what it is.
        specs = parse_version_specs(",".join(DEFAULT_VERSIONS), FALLBACK)

        self.assertEqual(
            [
                ("0.5.0", "lance-namespace==0.8.6"),
                ("0.4.2", "lance-namespace==0.7.5"),
                ("0.3.0", "lance-namespace==0.7.5"),
                ("0.2.0", "lance-namespace==0.4.5"),
            ],
            [(s.version, s.lance_namespace_spec) for s in specs],
        )

    def test_every_default_row_pins_explicitly(self):
        # A default row that forgot its '=' would silently inherit the
        # fallback pin, which cannot satisfy lance-ray 0.5.0.
        for entry in DEFAULT_VERSIONS:
            self.assertIn("=", entry, f"{entry} carries no lance-namespace pin")


if __name__ == "__main__":
    unittest.main()
