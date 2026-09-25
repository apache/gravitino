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

"""Test warning-only image reports and the bounded workflow contract."""

import contextlib
import io
import json
import os
import subprocess
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path
from unittest.mock import patch

import container_image_report as images


class TestContainerAdvisories(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        self.image = "docker.io/library/ubuntu:22.04"
        self.sbom = {
            "source": {
                "metadata": {
                    "userInput": self.image,
                    "architecture": "amd64",
                    "manifestDigest": "sha256:" + "a" * 64,
                }
            },
            "artifacts": [{"type": "deb"}],
        }
        self.scan = {"matches": []}

    def write_reports(self):
        (self.directory / "sbom.json").write_text(json.dumps(self.sbom))
        (self.directory / "vulnerabilities.json").write_text(json.dumps(self.scan))

    def report(self, sbom_outcome="success", scan_outcome="success"):
        with contextlib.redirect_stderr(io.StringIO()):
            return images.scan_summary(
                self.image, self.directory, sbom_outcome, scan_outcome
            )

    def test_critical_and_unfixed_findings_return_success_with_warning(self):
        self.scan["matches"] = [
            {"vulnerability": {"severity": "Critical", "fix": {"state": "not-fixed"}}}
        ]
        self.write_reports()
        out, err = io.StringIO(), io.StringIO()
        args = [
            images.SCRIPT,
            "--image",
            self.image,
            "--directory",
            str(self.directory),
        ]
        with patch.object(images.sys, "argv", args), contextlib.redirect_stdout(
            out
        ), contextlib.redirect_stderr(err):
            self.assertEqual(0, images.main())
        self.assertIn("| critical | 1 |", out.getvalue())
        self.assertIn("::warning", err.getvalue())

    def test_missing_or_failed_scan_is_advisory_and_not_clean(self):
        summary = self.report("failure", "skipped")
        self.assertIn("Incomplete scan", summary)
        self.assertIn("No clean result", summary)
        self.assertIn("Incomplete or invalid scan evidence", summary)

    def test_wrong_image_and_platform_are_visible_warnings(self):
        self.sbom["source"]["metadata"]["userInput"] = "postgres:13"
        self.write_reports()
        self.assertIn("differs from requested", self.report())
        self.sbom["source"]["metadata"]["userInput"] = self.image
        self.sbom["source"]["metadata"]["architecture"] = "arm64"
        self.write_reports()
        self.assertIn("not for the Linux amd64", self.report())

    def test_empty_inventory_warns_and_records_resolved_digest(self):
        self.sbom["artifacts"] = []
        self.write_reports()
        summary = self.report()
        self.assertIn("No packages were inventoried", summary)
        self.assertIn("sha256:" + "a" * 64, summary)

    def test_rustfs_missing_rust_metadata_is_advisory(self):
        self.image = "docker.io/rustfs/rustfs:1.0.0-rc.6"
        self.sbom["source"]["metadata"]["userInput"] = self.image
        self.write_reports()
        self.assertIn("embedded Rust dependency CVEs are not covered", self.report())

    def test_malformed_reports_warn(self):
        self.write_reports()
        (self.directory / "vulnerabilities.json").write_text("not JSON")
        self.assertIn("Incomplete or invalid", self.report())

    def test_empty_findings_do_not_claim_complete_coverage(self):
        self.write_reports()
        summary = self.report()
        self.assertIn(
            "No known vulnerability matches among inventoried packages", summary
        )
        self.assertIn("not complete dependency coverage", summary)

    def test_workflow_keeps_scans_advisory(self):
        root = Path(__file__).resolve().parents[2]
        workflow = (
            root / ".github/workflows/container-image-advisories.yml"
        ).read_text()
        self.assertIn('cron: "23 7 1 * *"', workflow)
        self.assertIn("workflow_dispatch:", workflow)
        self.assertIn("workflow_call:", workflow)
        self.assertNotIn("pull_request:", workflow)
        self.assertNotIn("push:", workflow)
        self.assertNotIn("issues: write", workflow)
        self.assertNotIn("security-events: write", workflow)
        self.assertIn("continue-on-error: true", workflow)
        self.assertIn("fail-build: false", workflow)
        self.assertIn("dependency-snapshot: false", workflow)
        self.assertIn("upload-release-assets: false", workflow)
        self.assertIn("dev/ci/container-scan-images.json", workflow)
        publisher = (root / ".github/workflows/docker-image.yml").read_text()
        self.assertIn("needs: publish-docker-image", publisher)
        self.assertIn(
            "uses: ./.github/workflows/container-image-advisories.yml", publisher
        )
        self.assertIn(
            "image: " + chr(36) + "{{ needs.publish-docker-image.outputs.image_ref }}",
            publisher,
        )

    def test_explicit_list_covers_products_and_tests_without_duplicates(self):
        root = Path(__file__).resolve().parents[2]
        entries = json.loads((root / "dev/ci/container-scan-images.json").read_text())[
            "images"
        ]
        references = [images.normalize(entry["image"]) for entry in entries]
        self.assertTrue(1 <= len(references) <= 256)
        self.assertEqual(len(references), len(set(references)))
        for entry in entries:
            self.assertTrue(entry["purpose"])
            self.assertTrue(entry["source"])
        for product in (
            "gravitino",
            "gravitino-iceberg-rest",
            "gravitino-lance-rest",
            "gravitino-mcp-server",
        ):
            self.assertTrue(
                any(
                    ref.startswith("docker.io/apache/" + product + ":")
                    for ref in references
                )
            )
        for fixture in (
            "rustfs/rustfs",
            "library/postgres",
            "datastrato/sample-authorization-server",
        ):
            self.assertTrue(
                any(ref.startswith("docker.io/" + fixture + ":") for ref in references)
            )

    def test_reference_normalization(self):
        self.assertEqual("docker.io/library/ubuntu:latest", images.normalize("ubuntu"))
        self.assertEqual(
            "docker.io/library/ubuntu:22.04", images.normalize("ubuntu:22.04")
        )
        self.assertEqual(
            "localhost:5000/test:1", images.normalize("localhost:5000/test:1")
        )
        with self.assertRaises(ValueError):
            images.normalize("invalid image")

    def test_annotations_escape_control_characters(self):
        output = io.StringIO()
        with contextlib.redirect_stderr(output):
            images.warning("message%\r\n::error::injected")
        self.assertIn("message%25%0D%0A::error::injected", output.getvalue())
        self.assertEqual(1, len(output.getvalue().splitlines()))

    def select_targets(self, entries, requested=""):
        root = Path(__file__).resolve().parents[2]
        workflow = (
            root / ".github/workflows/container-image-advisories.yml"
        ).read_text()
        script = textwrap.dedent(
            workflow.split("python3 - <<'PY'\n", 1)[1].split("\n          PY", 1)[0]
        )
        config = self.directory / "dev/ci"
        config.mkdir(parents=True, exist_ok=True)
        (config / "container-scan-images.json").write_text(
            json.dumps({"images": entries})
        )
        output = self.directory / "output"
        result = subprocess.run(
            [sys.executable, "-c", script],
            cwd=self.directory,
            env=dict(os.environ, IMAGE=requested, GITHUB_OUTPUT=str(output)),
            capture_output=True,
            text=True,
        )
        return result, output

    def test_scheduled_selection_reads_list(self):
        result, output = self.select_targets(
            [{"image": "ubuntu:22.04"}, {"image": "postgres:16"}]
        )
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual(
            {"image": ["ubuntu:22.04", "postgres:16"]},
            json.loads(output.read_text().removeprefix("matrix=")),
        )

    def test_manual_and_publication_selection_use_requested_image(self):
        requested = "example/image@sha256:" + "a" * 64
        result, output = self.select_targets([], requested)
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual(
            {"image": [requested]},
            json.loads(output.read_text().removeprefix("matrix=")),
        )

    def test_duplicate_and_empty_lists_fail_without_partial_matrix(self):
        for entries in (
            [],
            [{"image": "same"}] * 2,
            [{"image": str(i)} for i in range(257)],
        ):
            with self.subTest(entries=len(entries)):
                result, output = self.select_targets(entries)
                self.assertNotEqual(0, result.returncode)
                self.assertFalse(output.exists())


if __name__ == "__main__":
    unittest.main()
