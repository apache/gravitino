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

import copy
from collections import Counter
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest


SCRIPT_PATH = Path(__file__).parents[1] / "core_test_identity.py"
SPEC = importlib.util.spec_from_file_location("core_test_identity", SCRIPT_PATH)
core_test_identity = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(core_test_identity)

FIXTURES = Path(__file__).parent / "fixtures" / "core_test_identity"


def identity_counter(manifest):
    """Return the manifest identities in their natural Counter form."""
    return Counter(
        {
            (record["classname"], record["name"]): record["count"]
            for record in manifest["identities"]
        }
    )


def write_report(directory, testcases):
    """Write a minimal Gradle-compatible JUnit XML report."""
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "TEST-fixture.xml").write_text(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
        f"<testsuite name=\"fixture\">{testcases}</testsuite>\n",
        encoding="utf-8",
    )


class TestCoreTestIdentity(unittest.TestCase):
    def test_manifest_normalizes_database_identities_as_multisets(self):
        manifests = {
            lane: core_test_identity.build_manifest(lane, FIXTURES / lane)
            for lane in core_test_identity.LANES
        }

        database_counters = [
            identity_counter(manifests[lane])
            for lane in core_test_identity.DATABASE_LANES
        ]
        self.assertEqual(database_counters[0], database_counters[1])
        self.assertEqual(database_counters[0], database_counters[2])
        self.assertEqual(
            database_counters[0][
                (
                    "org.apache.gravitino.stats.storage."
                    "TestJdbcPartitionStatisticStorageIT$BackendTest",
                    "writesPartitionStats()[INDEX]",
                )
            ],
            2,
        )
        self.assertIn(
            (
                "org.apache.gravitino.TestCatalogMetaService",
                "testCreateCatalog()[BACKEND Backend]",
            ),
            database_counters[0],
        )
        self.assertIn(
            (
                "org.apache.gravitino.TestCatalogMetaService",
                "testDropCatalog()[BACKEND Backend]",
            ),
            database_counters[0],
        )
        self.assertIn(
            ("org.apache.gravitino.BackendTokenTest", "roundTrip[BACKEND]"),
            database_counters[0],
        )
        self.assertIn(
            (
                "org.apache.gravitino.UnmarkedBackendTest",
                "unmarkedSharedCase()",
            ),
            database_counters[0],
        )

        unit = manifests["unit"]
        self.assertEqual(unit["test_count"], 3)
        self.assertEqual(unit["duration_seconds"], 0.7)
        self.assertEqual(unit["status_counts"]["passed"], 2)
        self.assertEqual(unit["status_counts"]["skipped"], 1)
        self.assertEqual(unit["source_files"], ["TEST-unit.xml"])
        self.assertIn(
            (
                "org.apache.gravitino.TestH2ExceptionConverter",
                "testH2Converter()",
            ),
            identity_counter(unit),
        )
        self.assertIn(
            (
                "org.apache.gravitino.storage.relational.mapper.provider.postgresql."
                "TestCatalogMetaPostgreSQLProvider",
                "testInsertSql()",
            ),
            identity_counter(unit),
        )

    def test_lane_validation_rejects_explicit_wrong_backend_markers(self):
        cases = (
            (
                "unit",
                '<testcase classname="example.Test" '
                'name="runs()[H2 Backend]" time="0.1"/>',
                "Unit test result contains an explicit backend marker",
            ),
            (
                "h2",
                '<testcase classname="example.Test" '
                'name="runs()[MYSQL Backend]" time="0.1"/>',
                "foreign backend marker",
            ),
            (
                "postgresql",
                '<testcase classname="example.TestJdbcPartitionStatisticStorageIT$MySQLTest" '
                'name="runs()" time="0.1"/>',
                "foreign backend marker",
            ),
        )
        for lane, testcase, message in cases:
            with self.subTest(lane=lane), tempfile.TemporaryDirectory() as temp_dir:
                results = Path(temp_dir)
                write_report(results, testcase)
                with self.assertRaisesRegex(core_test_identity.ManifestError, message):
                    core_test_identity.build_manifest(lane, results)

    def test_manifest_fails_closed_on_missing_or_untrustworthy_results(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            with self.assertRaisesRegex(core_test_identity.ManifestError, "No TEST-"):
                core_test_identity.build_manifest("unit", temp_dir)

        invalid_cases = (
            ("<testsuite/>", "No <testcase>"),
            ("<testsuite>", "Could not parse"),
            (
                '<testsuite><testcase classname="example.Test" name="fails()" '
                'time="0.1"><failure/></testcase></testsuite>',
                "contains 1 failure",
            ),
            (
                '<testsuite><testcase classname="example.Test" name="errors()" '
                'time="0.1"><error/></testcase></testsuite>',
                "and 1 error",
            ),
        )
        for xml, message in invalid_cases:
            with self.subTest(message=message), tempfile.TemporaryDirectory() as temp_dir:
                results = Path(temp_dir)
                (results / "TEST-invalid.xml").write_text(xml, encoding="utf-8")
                with self.assertRaisesRegex(core_test_identity.ManifestError, message):
                    core_test_identity.build_manifest("unit", results)

    def test_reconcile_emits_combined_timing_and_identity_summary(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            output_directory = Path(temp_dir)
            manifest_files = []
            for lane in core_test_identity.LANES:
                manifest = core_test_identity.build_manifest(lane, FIXTURES / lane)
                manifest_file = output_directory / f"{lane}.json"
                core_test_identity.write_json(manifest, manifest_file)
                manifest_files.append(manifest_file)

            summary = core_test_identity.reconcile_manifests(manifest_files)

        self.assertTrue(summary["successful"])
        self.assertTrue(summary["database_identities_equal"])
        self.assertTrue(summary["unit_database_disjoint"])
        self.assertEqual(summary["database_test_count_per_lane"], 6)
        self.assertEqual(summary["database_unique_identity_count"], 5)
        self.assertEqual(summary["combined_test_count"], 21)
        self.assertEqual(summary["combined_duration_seconds"], 7.3)
        self.assertEqual(summary["combined_status_counts"]["skipped"], 2)
        self.assertEqual(set(summary["lanes"]), set(core_test_identity.LANES))
        self.assertEqual(
            summary["lanes"]["h2"]["source_files"], ["TEST-backend.xml"]
        )

    def test_reconcile_requires_exactly_four_matching_lanes(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            output_directory = Path(temp_dir)
            manifests = {}
            for lane in core_test_identity.LANES:
                manifest = core_test_identity.build_manifest(lane, FIXTURES / lane)
                manifest_file = output_directory / f"{lane}.json"
                core_test_identity.write_json(manifest, manifest_file)
                manifests[lane] = manifest_file

            with self.assertRaisesRegex(core_test_identity.ManifestError, "exactly 4"):
                core_test_identity.reconcile_manifests(list(manifests.values())[:3])

            mismatched = copy.deepcopy(
                core_test_identity.build_manifest("mysql", FIXTURES / "mysql")
            )
            mismatched["identities"][0]["name"] += "-different"
            mismatched["identity_digest"] = core_test_identity._identity_digest(
                identity_counter(mismatched)
            )
            mismatched_file = output_directory / "mysql-mismatched.json"
            core_test_identity.write_json(mismatched, mismatched_file)
            with self.assertRaisesRegex(
                core_test_identity.ManifestError, "Database identity mismatch"
            ):
                core_test_identity.reconcile_manifests(
                    [
                        manifests["unit"],
                        manifests["h2"],
                        mismatched_file,
                        manifests["postgresql"],
                    ]
                )

            with self.assertRaisesRegex(
                core_test_identity.ManifestError, "more than one manifest for lane h2"
            ):
                core_test_identity.reconcile_manifests(
                    [
                        manifests["unit"],
                        manifests["h2"],
                        manifests["h2"],
                        manifests["postgresql"],
                    ]
                )

    def test_reconcile_rejects_unit_database_overlap(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            output_directory = Path(temp_dir)
            overlapping_results = output_directory / "overlapping-unit-results"
            write_report(
                overlapping_results,
                '<testcase classname="org.apache.gravitino.UnmarkedBackendTest" '
                'name="unmarkedSharedCase()" time="0.1"/>',
            )

            manifest_files = []
            for lane in core_test_identity.LANES:
                results = (
                    overlapping_results if lane == "unit" else FIXTURES / lane
                )
                manifest = core_test_identity.build_manifest(lane, results)
                manifest_file = output_directory / f"{lane}.json"
                core_test_identity.write_json(manifest, manifest_file)
                manifest_files.append(manifest_file)

            with self.assertRaisesRegex(
                core_test_identity.ManifestError, "Unit/database identity overlap"
            ):
                core_test_identity.reconcile_manifests(manifest_files)

    def test_reconcile_rejects_tampered_identity_evidence(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            output_directory = Path(temp_dir)
            manifest_files = []
            for lane in core_test_identity.LANES:
                manifest = core_test_identity.build_manifest(lane, FIXTURES / lane)
                if lane == "h2":
                    manifest["identity_digest"] = "0" * 64
                manifest_file = output_directory / f"{lane}.json"
                core_test_identity.write_json(manifest, manifest_file)
                manifest_files.append(manifest_file)

            with self.assertRaisesRegex(
                core_test_identity.ManifestError, "invalid identity_digest"
            ):
                core_test_identity.reconcile_manifests(manifest_files)

    def test_cli_writes_manifest_and_reconciliation_output(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            output_directory = Path(temp_dir)
            manifest_files = []
            for lane in core_test_identity.LANES:
                manifest_file = output_directory / f"{lane}.json"
                return_code = core_test_identity.main(
                    [
                        "manifest",
                        "--lane",
                        lane,
                        "--results",
                        str(FIXTURES / lane),
                        "--output",
                        str(manifest_file),
                    ]
                )
                self.assertEqual(return_code, 0)
                self.assertTrue(manifest_file.is_file())
                manifest_files.append(manifest_file)

            summary_file = output_directory / "summary.json"
            return_code = core_test_identity.main(
                [
                    "reconcile",
                    "--manifests",
                    *(str(path) for path in manifest_files),
                    "--output",
                    str(summary_file),
                ]
            )
            self.assertEqual(return_code, 0)
            with summary_file.open(encoding="utf-8") as source:
                summary = json.load(source)
            self.assertTrue(summary["database_identities_equal"])


if __name__ == "__main__":
    unittest.main()
