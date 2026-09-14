#!/usr/bin/env python3
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

"""Regression checks for RustFS image update and vulnerability reporting."""

import copy
import unittest

import rustfs_image


def image(tag="1.0.0-rc.6"):
    return "rustfs/rustfs:" + tag + "@sha256:" + "a" * 64


def release(tag, prerelease=False, draft=False):
    return {"tag_name": tag, "prerelease": prerelease, "draft": draft}


class TestRustFSImage(unittest.TestCase):
    def setUp(self):
        self.sbom = {
            "source": {
                "type": "image",
                "metadata": {
                    "userInput": "registry:" + image(),
                    "architecture": "amd64",
                },
            },
            "artifacts": [{"type": "deb", "foundBy": "dpkg-db-cataloger"}],
        }

    def test_reads_the_consumed_compose_pin(self):
        self.assertEqual(
            image(),
            rustfs_image.read_image({"services": {"rustfs": {"image": image()}}}),
        )

    def test_rejects_mutable_or_unexpected_images(self):
        invalid = [
            "rustfs/rustfs:latest",
            "rustfs/rustfs:1.0.0-rc.6",
            "rustfs/rustfs@sha256:" + "a" * 64,
            image().replace("rustfs/rustfs", "other/rustfs"),
            image()[:-1],
            image() + "\nreference=other",
            image("latest"),
            image("1.0.0-rc.6;echo"),
        ]
        for reference in invalid:
            with self.subTest(reference=reference), self.assertRaises(ValueError):
                rustfs_image.image_version(reference)

    def test_missing_compose_service_fails(self):
        with self.assertRaises(KeyError):
            rustfs_image.read_image({"services": {"other": {"image": image()}}})

    def test_newer_release_candidate_is_detected_numerically(self):
        releases = [
            release("1.0.0-rc.6", True),
            release("1.0.0-rc.10", True),
            release("1.0.0-rc.9", True),
        ]
        self.assertEqual("1.0.0-rc.10", rustfs_image.newer_release(image(), releases))

    def test_stable_release_is_newer_than_its_candidate(self):
        releases = [release("1.0.0-rc.6", True), release("1.0.0")]
        self.assertEqual("1.0.0", rustfs_image.newer_release(image(), releases))
        summary, failed = rustfs_image.release_summary(image(), releases)
        self.assertTrue(failed)
        self.assertIn("https://github.com/rustfs/rustfs/releases/tag/1.0.0", summary)
        self.assertIn(image(), summary)

    def test_stable_pin_does_not_return_to_prereleases(self):
        releases = [
            release("1.0.0"),
            release("1.1.0-rc.1", True),
            release("2.0.0", True),
        ]
        self.assertIsNone(rustfs_image.newer_release(image("1.0.0"), releases))

    def test_v_prefix_and_patch_releases(self):
        releases = [release("v1.0.0"), release("v1.0.1")]
        self.assertEqual("v1.0.1", rustfs_image.newer_release(image("1.0.0"), releases))

    def test_drafts_and_unsupported_tags_are_not_recommendations(self):
        releases = [
            release("1.0.0-rc.6", True),
            release("1.0.0", draft=True),
            release("nightly"),
        ]
        self.assertIsNone(rustfs_image.newer_release(image(), releases))

    def test_current_pin_and_no_downgrades(self):
        releases = [release("1.0.0-rc.6", True), release("1.0.0-rc.5", True)]
        summary, failed = rustfs_image.release_summary(image(), releases)
        self.assertFalse(failed)
        self.assertIn("No newer supported release", summary)

    def test_empty_or_changed_release_response_is_an_error(self):
        for releases in ([], [release("nightly")]):
            with self.subTest(releases=releases), self.assertRaises(ValueError):
                rustfs_image.newer_release(image(), releases)

    def test_clean_os_scan_reports_missing_rust_coverage(self):
        summary, failed = rustfs_image.vulnerability_summary(
            image(), self.sbom, {"matches": []}
        )
        self.assertFalse(failed)
        self.assertIn("Coverage gap", summary)
        self.assertIn("Rust packages: 0", summary)

    def test_rust_lockfile_does_not_imply_auditable_binary_coverage(self):
        self.sbom["artifacts"].append(
            {"type": "rust-crate", "foundBy": "rust-cargo-lock-cataloger"}
        )
        summary, _ = rustfs_image.vulnerability_summary(
            image(), self.sbom, {"matches": []}
        )
        self.assertIn("Rust packages: 1", summary)
        self.assertIn("Coverage gap", summary)

    def test_auditable_packages_are_reported_without_claiming_complete_coverage(self):
        self.sbom["artifacts"].append(
            {"type": "rust-crate", "foundBy": "cargo-auditable-binary-cataloger"}
        )
        summary, failed = rustfs_image.vulnerability_summary(
            image(), self.sbom, {"matches": []}
        )
        self.assertFalse(failed)
        self.assertIn("cargo-auditable binaries: 1", summary)
        self.assertNotIn("Coverage gap", summary)
        self.assertIn("not proof that every dependency was detected", summary)

    def test_high_and_critical_findings_require_triage_even_without_fixes(self):
        for severity in ("High", "Critical"):
            with self.subTest(severity=severity):
                scan = {
                    "matches": [
                        {
                            "vulnerability": {
                                "severity": severity,
                                "fix": {"state": "not-fixed"},
                            }
                        }
                    ]
                }
                summary, failed = rustfs_image.vulnerability_summary(
                    image(), self.sbom, scan
                )
                self.assertTrue(failed)
                self.assertIn("Maintainer action required", summary)
                self.assertIn("| " + severity.lower() + " | 1 |", summary)

    def test_lower_severity_findings_are_retained(self):
        scan = {"matches": [{"vulnerability": {"severity": "Medium"}}]}
        summary, failed = rustfs_image.vulnerability_summary(image(), self.sbom, scan)
        self.assertFalse(failed)
        self.assertIn("| medium | 1 |", summary)

    def test_empty_inventory_is_not_a_clean_scan(self):
        self.sbom["artifacts"] = []
        with self.assertRaisesRegex(ValueError, "no packages"):
            rustfs_image.vulnerability_summary(image(), self.sbom, {"matches": []})

    def test_wrong_platform_does_not_claim_ci_image_coverage(self):
        self.sbom["source"]["metadata"]["architecture"] = "arm64"
        with self.assertRaisesRegex(ValueError, "amd64"):
            rustfs_image.vulnerability_summary(image(), self.sbom, {"matches": []})

    def test_wrong_image_or_missing_scan_data_is_not_a_clean_scan(self):
        other_sbom = copy.deepcopy(self.sbom)
        other_sbom["source"]["metadata"]["userInput"] = image("1.0.0-rc.5")
        with self.assertRaisesRegex(ValueError, "pinned image"):
            rustfs_image.vulnerability_summary(image(), other_sbom, {"matches": []})
        with self.assertRaises(KeyError):
            rustfs_image.vulnerability_summary(image(), self.sbom, {})


if __name__ == "__main__":
    unittest.main()
