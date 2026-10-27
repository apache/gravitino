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

"""Regression checks for CI task ownership and catch-all exclusions."""

import json
from pathlib import Path
import subprocess
import os
import re
import shutil
import tempfile
import textwrap
import unittest

SCRIPT = Path(__file__).with_name("test-shards.sh")
ROOT = SCRIPT.parents[2]


def arguments(suite, shard):
    return subprocess.check_output(
        ["bash", str(SCRIPT), suite, shard], text=True
    ).splitlines()


class TestShardTasks(unittest.TestCase):
    def test_build_leaves_core_to_dedicated_lanes(self):
        shards = json.loads(
            subprocess.check_output(["bash", str(SCRIPT), "build", "--list"], text=True)
        )
        self.assertEqual(["docker", "others"], shards)
        for shard in shards:
            self.assertNotIn(":core:test", arguments("build", shard))
        # Every build shard receives the workflow's global :core:test exclusion.
        workflow = SCRIPT.parents[2] / ".github/workflows/build.yml"
        self.assertIn("-x :core:test", workflow.read_text())

    def test_each_named_task_has_exactly_one_owner_and_is_excluded_from_others(self):
        for suite, root in [("build", "build"), ("backend-it", "test")]:
            with self.subTest(suite=suite):
                shards = json.loads(
                    subprocess.check_output(
                        ["bash", str(SCRIPT), suite, "--list"], text=True
                    )
                )
                self.assertEqual("others", shards[-1])
                tasks = [
                    task for shard in shards[:-1] for task in arguments(suite, shard)
                ]
                self.assertTrue(tasks)
                self.assertEqual(len(tasks), len(set(tasks)))
                others = arguments(suite, "others")
                self.assertEqual(root, others[0])
                self.assertEqual(["-x"] * len(tasks), others[1::2])
                self.assertEqual(set(tasks), set(others[2::2]))

    def test_backend_lakehouse_owns_experimental_iceberg_tests(self):
        task = ":iceberg:iceberg-rest-server:experimentalTest"
        self.assertIn(task, arguments("backend-it", "lakehouse"))
        self.assertNotIn(task, arguments("backend-it", "hive"))
        others = arguments("backend-it", "others")
        self.assertIn(task, others[2::2])
        self.assertTrue(
            all(
                not t.endswith(":experimentalTest")
                for t in arguments("build", "docker")
            )
        )

    def test_cron_preserves_the_complete_backend_suite(self):
        self.assertEqual(
            ["test", ":iceberg:iceberg-rest-server:experimentalTest"],
            arguments("backend-it", "all"),
        )
        cron = SCRIPT.parents[2] / ".github/workflows/cron-integration-test.yml"
        self.assertIn("      shard: all", cron.read_text())

    def test_unknown_suite_or_shard_fails(self):
        for suite, shard in [
            ("unknown", "--list"),
            ("build", "core"),
            ("build", "all"),
            ("backend-it", "unknown"),
            ("flink", "mysql"),
            ("access-control", "embedded"),
        ]:
            with self.subTest(suite=suite, shard=shard):
                result = subprocess.run(
                    ["bash", str(SCRIPT), suite, shard],
                    text=True,
                    capture_output=True,
                    check=False,
                )
                self.assertNotEqual(0, result.returncode)
                self.assertIn("Unknown", result.stderr)
                self.assertEqual("", result.stdout)


class TestIntegrationShardExecution(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="gravitino-test-shards-")
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        (self.root / "dev/ci").mkdir(parents=True)
        for script in ("test-shards.sh", "run-integration-shard.sh"):
            shutil.copy(SCRIPT.with_name(script), self.root / "dev/ci" / script)
        self.log = self.root / "calls.jsonl"
        gradlew = self.root / "gradlew"
        gradlew.write_text(
            "#!/usr/bin/env python3\n"
            "import json, os, sys\n"
            "with open(os.environ['SHARD_TEST_LOG'], 'a') as stream:\n"
            "    stream.write(json.dumps(sys.argv[1:]) + '\\n')\n"
            "sys.exit(int(os.environ.get('SHARD_TEST_EXIT', '0')))\n"
        )
        gradlew.chmod(0o755)
        self.env = {**os.environ, "SHARD_TEST_LOG": str(self.log)}

    def execute(self, suite, shard, exit_code=0):
        result = subprocess.run(
            ["bash", str(self.root / "dev/ci/run-integration-shard.sh"), suite, shard],
            cwd=self.root,
            env=self.env,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(exit_code, result.returncode, result.stderr)
        return (
            [json.loads(line) for line in self.log.read_text().splitlines()]
            if self.log.exists()
            else []
        )

    def execute_workflow(self, filename, step_name, expressions, exit_code=0):
        source = (ROOT / ".github/workflows" / filename).read_text()
        # Build has a compile-only step with the same display name; test its sharded job.
        if filename == "build.yml":
            source = source.split("\n  build:\n", 1)[1]
        step = source.split("      - name: " + step_name + "\n", 1)[1]
        body = step.split("        run: |\n", 1)[1]
        lines = []
        for line in body.splitlines():
            if line.strip() and not line.startswith("          "):
                break
            lines.append(line)
        script = textwrap.dedent("\n".join(lines))
        script = re.sub(
            r"\$\{\{\s*(.*?)\s*\}\}", lambda match: expressions[match[1]], script
        )
        result = subprocess.run(
            ["bash", "-e", "-o", "pipefail", "-c", script],
            cwd=self.root,
            env=self.env,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(exit_code, result.returncode, result.stderr)
        return (
            [json.loads(line) for line in self.log.read_text().splitlines()]
            if self.log.exists()
            else []
        )

    def test_backend_workflow_executes_all_shards_with_existing_filters(self):
        (self.root / "catalogs-contrib/catalog-future").mkdir(parents=True)
        task_owners = []
        for backend, mode in [
            ("h2", "embedded"),
            ("mysql", "deploy"),
            ("postgresql", "deploy"),
        ]:
            for shard in ("hive", "lakehouse", "others"):
                self.log.unlink(missing_ok=True)
                calls = self.execute_workflow(
                    "backend-integration-test-action.yml",
                    "Backend Integration Test (JDK${{ inputs.java-version }}-${{ inputs.test-mode }}-${{ inputs.backend }})",
                    {
                        "inputs.shard": shard,
                        "inputs.test-mode": mode,
                        "inputs.backend": backend,
                    },
                )
                self.assertEqual(1, len(calls))
                args = calls[0]
                selected = arguments("backend-it", shard)
                self.assertEqual(selected, args[: len(selected)])
                self.assertIn("-PskipTests", args)
                self.assertIn("-PtestMode=" + mode, args)
                self.assertIn("-PjdbcBackend=" + backend, args)
                self.assertIn("-PskipDockerTests=false", args)
                excluded = [
                    args[i + 1] for i, value in enumerate(args) if value == "-x"
                ]
                self.assertIn(":catalogs-contrib:catalog-future:test", excluded)
                self.assertIn(":flink-connector:flink-2.2:test", excluded)
                self.assertIn(":plugins:maintenance:test", excluded)
                self.assertIn(":clients:client-python:test", excluded)
                if shard != "others" and backend == "h2":
                    task_owners.extend(selected)
        self.assertEqual(len(task_owners), len(set(task_owners)))
        self.assertIn(":iceberg:iceberg-rest-server:experimentalTest", task_owners)

    def test_build_workflow_executes_shards_and_preserves_conditional_paths(self):
        for maintenance in ("false", "true"):
            for mcp in ("false", "true"):
                for shard in ("docker", "others"):
                    with self.subTest(maintenance=maintenance, mcp=mcp, shard=shard):
                        self.log.unlink(missing_ok=True)
                        calls = self.execute_workflow(
                            "build.yml",
                            "Build with Gradle",
                            {
                                "needs.changes.outputs.maintenance_module_only_changes": maintenance,
                                "needs.changes.outputs.mcp_server_changes": mcp,
                                "matrix.shard": shard,
                            },
                        )
                        if maintenance == "true" and shard == "docker":
                            self.assertEqual([], calls)
                            continue
                        self.assertEqual(1, len(calls))
                        args = calls[0]
                        self.assertIn("-PskipITs", args)
                        self.assertIn("-PskipDockerTests=false", args)
                        if maintenance == "true":
                            self.assertIn(":maintenance:maintenance-common:build", args)
                            continue
                        selected = arguments("build", shard)
                        self.assertEqual(selected, args[: len(selected)])
                        excluded = [
                            args[i + 1] for i, value in enumerate(args) if value == "-x"
                        ]
                        self.assertIn(":core:test", excluded)
                        self.assertEqual(mcp == "false", ":mcp-server:test" in excluded)

    def test_flink_modes_cover_every_registered_version_once(self):
        settings = (ROOT / "settings.gradle.kts").read_text()
        versions = set(
            re.findall(r'project\(":flink-connector:flink-(\d+\.\d+)"\)', settings)
        )
        self.assertTrue(versions)
        selected = []
        modes = json.loads(
            subprocess.check_output(["bash", str(SCRIPT), "flink", "--list"], text=True)
        )
        self.assertCountEqual(("embedded", "deploy"), modes)
        for mode in modes:
            self.log.unlink(missing_ok=True)
            calls = self.execute("flink", mode)
            self.assertEqual(len(versions), len(calls))
            for args in calls:
                self.assertIn("-PskipTests", args)
                self.assertIn("-PtestMode=" + mode, args)
                self.assertIn("-PskipDockerTests=false", args)
                self.assertIn("-PskipWeb=true", args)
                self.assertEqual(
                    "org.apache.gravitino.flink.connector.integration.test.**",
                    args[args.index("--tests") + 1],
                )
                task = next(arg for arg in args if arg.endswith(":test"))
                version = re.fullmatch(r":flink-connector:flink-(.+):test", task)[1]
                selected.append((mode, version))
        expected = {
            (mode, version) for mode in ("embedded", "deploy") for version in versions
        }
        self.assertEqual(expected, set(selected))
        self.assertEqual(len(expected), len(selected))
        self.log.unlink()
        all_calls = self.execute("flink", "all")
        self.assertEqual(len(expected), len(all_calls))
        self.assertCountEqual(
            selected,
            [
                (
                    next(
                        arg.removeprefix("-PtestMode=")
                        for arg in args
                        if arg.startswith("-PtestMode=")
                    ),
                    re.fullmatch(
                        r":flink-connector:flink-(.+):test",
                        next(arg for arg in args if arg.endswith(":test")),
                    )[1],
                )
                for args in all_calls
            ],
        )

    def test_authorization_retains_all_backend_mode_combinations_and_unit_tests(self):
        expected = {"h2": "embedded", "mysql": "deploy", "postgresql": "deploy"}
        calls_by_shard = []
        backends = json.loads(
            subprocess.check_output(
                ["bash", str(SCRIPT), "access-control", "--list"], text=True
            )
        )
        self.assertCountEqual(expected, backends)
        for backend in backends:
            mode = expected[backend]
            self.log.unlink(missing_ok=True)
            calls = self.execute("access-control", backend)
            self.assertEqual(1, len(calls))
            self.assertEqual(
                [
                    "-PtestMode=" + mode,
                    "-PjdbcBackend=" + backend,
                    "-PskipDockerTests=false",
                    "-PskipWeb=true",
                    ":authorizations:test",
                ],
                calls[0],
            )
            self.assertNotIn("-PskipTests", calls[0])
            calls_by_shard.extend(calls)
        self.log.unlink()
        self.assertEqual(calls_by_shard, self.execute("access-control", "all"))

    def test_invalid_selections_start_no_gradle_process(self):
        for suite, shard in [
            ("unknown", "all"),
            ("build", "docker"),
            ("flink", "mysql"),
            ("access-control", "embedded"),
        ]:
            with self.subTest(suite=suite, shard=shard):
                self.assertEqual([], self.execute(suite, shard, exit_code=1))

    def test_gradle_failure_stops_the_shard_and_is_preserved(self):
        self.env["SHARD_TEST_EXIT"] = "23"
        for suite in ("flink", "access-control"):
            self.log.unlink(missing_ok=True)
            self.assertEqual(1, len(self.execute(suite, "all", exit_code=23)))


class TestWorkflowShardContracts(unittest.TestCase):
    def test_matrices_are_generated_and_pass_the_selected_shard(self):
        for filename, suite, output in [
            ("build.yml", "build", "build_shards"),
            ("backend-integration-test.yml", "backend-it", "backend_it_shards"),
            ("flink-integration-test.yml", "flink", "flink_shards"),
            (
                "access-control-integration-test.yml",
                "access-control",
                "access_control_shards",
            ),
        ]:
            with self.subTest(workflow=filename):
                source = (ROOT / ".github/workflows" / filename).read_text()
                self.assertIn("dev/ci/test-shards.sh " + suite + " --list", source)
                self.assertIn(
                    "shard: ${{ fromJSON(needs.changes.outputs." + output + ") }}",
                    source,
                )
                self.assertIn("fail-fast: false", source)
                self.assertIn("checkout-and-sync", source)
                if suite != "build":
                    self.assertIn("shard: ${{ matrix.shard }}", source)
                for key in (
                    "base-ref",
                    "base-sha",
                    "head-repository",
                    "head-sha",
                    "sync",
                ):
                    self.assertIn(key + ": ${{ inputs." + key + " }}", source)

    def test_runner_wiring_and_artifacts_include_the_shard(self):
        for suite in ("flink", "access-control"):
            source = (
                ROOT / ".github/workflows" / (suite + "-integration-test-action.yml")
            ).read_text()
            self.assertIn(
                "run: dev/ci/run-integration-shard.sh " + suite + ' "${TEST_SHARD}"',
                source,
            )
            self.assertIn("TEST_SHARD: ${{ inputs.shard }}", source)
            self.assertIn(
                "name: JDK${{ inputs.java-version }}-${{ inputs.shard }}", source
            )
            self.assertRegex(
                source, r"name: \S+-integrate-test-reports-.*\$\{\{ inputs.shard \}\}"
            )
            self.assertIn("default: 'all'", source)

    def test_cron_explicitly_selects_all_three_complete_suites(self):
        source = (ROOT / ".github/workflows/cron-integration-test.yml").read_text()
        for suite in ("backend", "flink", "access-control"):
            block = source.split("\n  " + suite + ":\n", 1)[1].split("\n\n", 1)[0]
            self.assertIn("      shard: all", block)

    def test_named_projects_exist_and_catch_all_covers_new_projects(self):
        # Independent project registry, rather than only checking one script-generated list
        # against another. A newly registered project must still belong to the catch-all.
        settings = (ROOT / "settings.gradle.kts").read_text()
        projects = {
            ":" + name.lstrip(":")
            for name in re.findall(r'"([\w:.-]+:[\w:.-]+)"', settings)
        }
        self.assertGreater(len(projects), 50)
        projects.add(":future:new-project")
        universe = {project + ":test" for project in projects}
        for suite in ("build", "backend-it"):
            shards = json.loads(
                subprocess.check_output(
                    ["bash", str(SCRIPT), suite, "--list"], text=True
                )
            )
            assigned = [
                task
                for shard in shards[:-1]
                for task in arguments(suite, shard)
                if task.endswith(":test")
            ]
            self.assertTrue(set(assigned).issubset(universe))
            others = arguments(suite, "others")[2::2]
            expanded = [task for task in universe if task not in others] + assigned
            self.assertCountEqual(universe, expanded)
            self.assertNotIn(":future:new-project:test", others)


if __name__ == "__main__":
    unittest.main()
