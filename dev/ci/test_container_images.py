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

"""Exercise discovery boundaries and the warning-only monitoring contract."""

import contextlib
import io
import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import container_images as images
import yaml


class TestContainerInventory(unittest.TestCase):
    def setUp(self):
        self.inventory = images.Inventory()

    def references(self):
        return set(self.inventory.images)

    def test_registry_normalization_and_mutable_tags(self):
        self.assertEqual("docker.io/library/mysql:8.0", images.normalize("mysql:8.0"))
        self.assertEqual(
            images.normalize("mysql:8.0"), images.normalize("docker.io/mysql:8.0")
        )
        self.assertEqual(
            "docker.io/localstack/localstack:latest",
            images.normalize("localstack/localstack"),
        )
        self.assertEqual(
            "registry.example:5000/team/image:1",
            images.normalize("registry.example:5000/team/image:1"),
        )
        pinned = "quay.io/team/image:1@sha256:" + "a" * 64
        self.assertEqual(pinned, images.normalize(pinned))

    def test_unsupported_and_injected_references_are_not_scan_targets(self):
        for value in (
            "${IMAGE}",
            "{{ .Values.image }}",
            "$(command)",
            "mysql:8\nother",
            "user:password@registry/image",
            "image@sha256:bad",
        ):
            with self.subTest(value=value), self.assertRaises(ValueError):
                images.normalize(value)

    def test_dockerfile_global_arguments_stage_aliases_and_scratch(self):
        text = "ARG BASE=ubuntu:22.04\nFROM ${BASE} AS build\nRUN echo \\\n  test\nFROM build AS finish\nFROM scratch\n"
        images.dockerfile_images("Dockerfile", text, self.inventory)
        self.assertEqual({"docker.io/library/ubuntu:22.04"}, self.references())
        self.assertEqual(
            2, self.inventory.images[images.normalize("ubuntu:22.04")][0]["line"]
        )

    def test_unknown_build_argument_is_recorded(self):
        images.dockerfile_images(
            "Dockerfile", "ARG BASE\nFROM ${BASE}\n", self.inventory
        )
        self.assertFalse(self.references())
        self.assertEqual("${BASE}", self.inventory.unresolved[0]["value"])

    def test_external_copy_image_is_included_but_build_stages_are_not(self):
        images.dockerfile_images(
            "Dockerfile",
            "FROM python:3.10 AS build\nCOPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv\nCOPY --from=build /app /app\nCOPY --from=0 /other /other\n",
            self.inventory,
        )
        self.assertEqual(
            {images.normalize("python:3.10"), "ghcr.io/astral-sh/uv:latest"},
            self.references(),
        )

    def test_compose_nested_defaults_do_not_read_runtime_environment(self):
        text = "services:\n  db:\n    image: ${OVERRIDE:-${IMAGE:-apache/gravitino-ci:doris-0.1.5}}\n  trino:\n    image: trinodb/trino:${VERSION:-478}\n"
        with patch.dict("os.environ", {"VERSION": "999", "IMAGE": "unexpected:latest"}):
            images.yaml_images("docker-compose.yaml", text, self.inventory)
        self.assertEqual(
            {
                images.normalize("apache/gravitino-ci:doris-0.1.5"),
                images.normalize("trinodb/trino:478"),
            },
            self.references(),
        )

    def test_locally_built_compose_service_is_not_pulled(self):
        images.yaml_images(
            "docker-compose.yml",
            "services:\n  ranger:\n    build: .\n    image: ranger-build\n",
            self.inventory,
        )
        self.assertFalse(self.references())
        self.assertIn("Locally built", self.inventory.unresolved[0]["reason"])

    def test_ci_services_job_containers_and_container_actions(self):
        text = "jobs:\n  test:\n    container: ubuntu:22.04\n    services:\n      db:\n        image: mysql:8.0.33\n    steps:\n      - uses: docker://alpine:3.20\n"
        images.yaml_images(".github/workflows/test.yml", text, self.inventory)
        self.assertEqual(
            {
                images.normalize(ref)
                for ref in ("ubuntu:22.04", "mysql:8.0.33", "alpine:3.20")
            },
            self.references(),
        )

    def test_helm_repository_tag_and_digest_defaults(self):
        text = (
            "image:\n  registry: docker.io\n  repository: apache/gravitino\n  tag: snapshot\nmysql:\n  image:\n    repository: bitnamilegacy/mysql\n    digest: sha256:"
            + "a" * 64
            + "\n"
        )
        images.yaml_images("dev/charts/example/values.yaml", text, self.inventory)
        self.assertIn(images.normalize("apache/gravitino:snapshot"), self.references())
        self.assertIn(
            images.normalize("bitnamilegacy/mysql@sha256:" + "a" * 64),
            self.references(),
        )

    def test_helm_computed_tag_and_template_expressions_are_disclosed(self):
        images.yaml_images(
            "dev/charts/example/values.yaml",
            "image:\n  repository: apache/gravitino\n  tag: ''\n",
            self.inventory,
        )
        images.yaml_images(
            "dev/charts/example/templates/deployment.yaml",
            'metadata: {{ include "name" . }}\nimage: "{{ .Values.image }}"\n',
            self.inventory,
        )
        self.assertFalse(self.references())
        self.assertEqual(2, len(self.inventory.unresolved))

    def test_static_helm_test_image_survives_unrendered_template(self):
        images.yaml_images(
            "dev/charts/example/templates/tests/check.yaml",
            'metadata: {{ include "name" . }}\nimage: curlimages/curl:latest\n',
            self.inventory,
        )
        self.assertEqual(
            {images.normalize("curlimages/curl:latest")}, self.references()
        )

    def test_yaml_alias_cycles_do_not_recurse_forever(self):
        images.yaml_images(
            "compose.yaml",
            "service: &service\n  image: postgres:13\n  self: *service\n",
            self.inventory,
        )
        self.assertEqual({images.normalize("postgres:13")}, self.references())

    def test_gradle_glob_strings_do_not_hide_later_image_defaults(self):
        text = 'val sources = "**/*"\n// ignored IMAGE = "bad:1"\nparam.environment("GRAVITINO_CI_KAFKA_DOCKER_IMAGE", "apache/kafka:3.7.0")\n/* documentation */\nval env = "GRAVITINO_CI_HIVE_DOCKER_IMAGE" to "apache/gravitino-ci:hive-0.1.13"\n'
        images.code_images("build.gradle.kts", text, self.inventory)
        self.assertEqual(
            {
                images.normalize("apache/kafka:3.7.0"),
                images.normalize("apache/gravitino-ci:hive-0.1.13"),
            },
            self.references(),
        )
        self.assertEqual(
            3, self.inventory.images[images.normalize("apache/kafka:3.7.0")][0]["line"]
        )

    def test_java_literals_calls_enums_and_runtime_overrides(self):
        text = 'String DEFAULT_IMAGE = "mysql:8.0";\nnew MySQLContainer<>("mysql:8.0.33");\nwithImage("mysql:5.7");\nDockerImageName.parse("mysql:8.4");\nString image = System.getenv("CI_IMAGE");\n'
        images.code_images("TestContainer.java", text, self.inventory)
        images.code_images(
            "PGImageName.java",
            'VERSION_12("postgres:12"), VERSION_13("postgres:13");',
            self.inventory,
        )
        self.assertEqual(6, len(self.references()))
        self.assertEqual("CI_IMAGE", self.inventory.unresolved[0]["value"])

    def test_docker_cli_flags_and_unknown_options(self):
        images.shell_images(
            "start.sh",
            "docker run -it -d --restart always --net host --name helper team/helper\ndocker pull postgres:16\ndocker run --unknown value image:1\n",
            self.inventory,
        )
        self.assertEqual(
            {images.normalize("team/helper"), images.normalize("postgres:16")},
            self.references(),
        )
        self.assertIn("image position unknown", self.inventory.unresolved[0]["reason"])

    def test_deduplication_keeps_source_locations(self):
        self.inventory.add("Dockerfile", 1, "mysql:8.0", "Dockerfile")
        self.inventory.add("compose.yaml", 5, "docker.io/library/mysql:8.0", "Compose")
        result = self.inventory.result()
        self.assertEqual(1, len(result["images"]))
        self.assertEqual(2, len(result["images"][0]["sources"]))

    def test_discovery_skips_docs_mocks_and_symlinks_and_accepts_future_rustfs(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            paths = [
                "docs/compose.yaml",
                "dev/charts/example/tests/example.yaml",
                "integration-test-common/src/test/resources/docker-compose-rustfs.yml",
            ]
            for path in paths:
                file = root / path
                file.parent.mkdir(parents=True, exist_ok=True)
                file.write_text(
                    "services:\n  rustfs:\n    image: rustfs/rustfs:1.0.0-rc.6@sha256:"
                    + "a" * 64
                    + "\n"
                )
            (root / "symlink.yaml").symlink_to(root / paths[0])
            result = images.discover(root, paths + ["symlink.yaml"])
            self.assertEqual(1, len(result["images"]))
            self.assertEqual(paths[-1], result["images"][0]["sources"][0]["path"])


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
            "report",
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

    def test_inventory_failure_returns_success_and_empty_matrix(self):
        args = [
            images.SCRIPT,
            "inventory",
            "--root",
            str(self.directory),
            "--output",
            str(self.directory / "out"),
            "--github-output",
            str(self.directory / "github-output"),
        ]
        with patch.object(images.sys, "argv", args), patch.object(
            images.subprocess,
            "check_output",
            side_effect=subprocess.CalledProcessError(1, "git"),
        ), contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(0, images.main())
        self.assertIn("count=0", (self.directory / "github-output").read_text())
        self.assertIn(
            "Declarations requiring review",
            (self.directory / "out/summary.md").read_text(),
        )

    def test_workflow_has_no_pr_gate_or_issue_writes(self):
        root = Path(__file__).resolve().parents[2]
        workflow = yaml.load(
            (root / images.WORKFLOW).read_text(), Loader=yaml.BaseLoader
        )
        self.assertEqual({"schedule", "workflow_dispatch"}, set(workflow["on"]))
        self.assertEqual({"contents": "read"}, workflow["permissions"])
        self.assertTrue(
            all(job["continue-on-error"] == "true" for job in workflow["jobs"].values())
        )
        steps = workflow["jobs"]["scan"]["steps"]
        scan = next(step for step in steps if step.get("id") == "scan")
        self.assertEqual("false", scan["with"]["fail-build"])
        self.assertEqual("true", scan["continue-on-error"])


if __name__ == "__main__":
    unittest.main()
