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

"""Check the Required CI parent/child contract without starting Actions.

This script does not contact GitHub. It reads the checked-in workflow
sources and asserts the contract this PR relies on:

- the parent is the sole pull_request listener for the aggregated suites
- each suite is reusable via workflow_call + required_ci and keeps push
- Required CI is a static always() aggregate that fails on any non-success
- conflict-marker-check stays standalone and is not part of the aggregate
- required-mode concurrency keys are unique per suite
"""

import re
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW_DIR = REPO_ROOT / ".github" / "workflows"
REQUIRED_CI_WORKFLOW = WORKFLOW_DIR / "required-ci.yml"
CONFLICT_MARKER_WORKFLOW = WORKFLOW_DIR / "conflict-marker-check.yml"

SUITE_WORKFLOWS = {
    "build": "build.yml",
    "backend": "backend-integration-test.yml",
    "spark": "spark-integration-test.yml",
    "flink": "flink-integration-test.yml",
    "trino": "trino-integration-test.yml",
    "iceberg_rest_trino": "iceberg-rest-trino-integration-test.yml",
    "python": "python-integration-test.yml",
    "frontend": "frontend-integration-test.yml",
    "access_control": "access-control-integration-test.yml",
    "idp_basic": "idp-basic-test.yml",
    "mcp": "mcp-integration-test.yml",
    "maintenance": "maintenance-integration-test.yml",
    "contrib_catalog": "contrib-catalog-test.yml",
    "web_ui": "web-ui-tests.yml",
}

REQUIRED_CONCURRENCY_KEYS = {
    "build": "required-build",
    "backend": "required-backend",
    "spark": "required-spark",
    "flink": "required-flink",
    "trino": "required-trino",
    "iceberg_rest_trino": "required-iceberg-rest-trino",
    "python": "required-python",
    "frontend": "required-frontend",
    "access_control": "required-access-control",
    "idp_basic": "required-idp-basic",
    "mcp": "required-mcp",
    "maintenance": "required-maintenance",
    "contrib_catalog": "required-contrib-catalog",
}

WORKFLOW_DISPATCH_SUITES = {
    "contrib-catalog-test.yml",
    "idp-basic-test.yml",
}


def check_equal(name, actual, expected):
    """Raise a readable error when a value differs from the contract."""
    if actual != expected:
        raise AssertionError(f"{name}: expected {expected!r}, got {actual!r}")


def job_block(source, job_id):
    """Return the source of one top-level job, up to the next job or EOF."""
    match = re.search(
        rf"(?m)^  {re.escape(job_id)}:\n(?P<body>(?:(?!^  [A-Za-z_]).*\n)*)",
        source,
    )
    if not match:
        raise AssertionError(f"missing job {job_id!r}")
    return match.group("body")


def static_job_name(job_source):
    """Return a job's static name, or None when the name is an expression."""
    match = re.search(r"(?m)^    name: (.+)$", job_source)
    if not match:
        return None
    name = match.group(1).strip()
    if name in {">-", "|"} or "${{" in name:
        return None
    return name


def aggregate_passes(suite_results):
    """Return whether the Required CI job may report success."""
    return all(result == "success" for result in suite_results.values())


def simulate_aggregate():
    """The aggregate is fail-closed on any non-success suite result."""
    successes = {job_id: "success" for job_id in SUITE_WORKFLOWS}
    cases = [
        ("all suites succeed", successes, True),
        ("suite fails", {**successes, "build": "failure"}, False),
        ("suite is cancelled", {**successes, "backend": "cancelled"}, False),
        ("suite is skipped", {**successes, "spark": "skipped"}, False),
    ]
    for name, suite_results, expected in cases:
        check_equal(name, aggregate_passes(suite_results), expected)
    print(f"Passed {len(cases)} aggregate-result cases")


def verify_parent():
    """Verify the parent owns fan-out and the one Required CI result."""
    source = REQUIRED_CI_WORKFLOW.read_text(encoding="utf-8")
    if "types: [opened, synchronize, reopened]" not in source:
        raise AssertionError("required-ci.yml: missing always-on pull_request types")
    if "cancel-in-progress: false" not in source:
        raise AssertionError("required-ci.yml: parent must not cancel in-progress runs")
    for forbidden in ("run-ci", "graphite-merge-queue", "gtmq_", "CI not requested"):
        if forbidden in source:
            raise AssertionError(
                f"required-ci.yml: enterprise routing leaked {forbidden!r}"
            )

    required_ci = job_block(source, "required_ci")
    check_equal("required_ci name", static_job_name(required_ci), "Required CI")
    if "if: always()" not in required_ci:
        raise AssertionError("required_ci must run with if: always()")
    if '.value.result != "success"' not in required_ci:
        raise AssertionError("required_ci must fail on any non-success suite")

    for job_id, workflow_name in SUITE_WORKFLOWS.items():
        call_pattern = re.compile(
            rf"(?ms)^  {job_id}:\n.*?"
            rf"uses: \./\.github/workflows/{re.escape(workflow_name)}\n.*?"
            r"required_ci: true\n.*?secrets: inherit"
        )
        if not call_pattern.search(source):
            raise AssertionError(
                f"required-ci.yml: missing configured call for {workflow_name}"
            )
        if f"- {job_id}" not in required_ci:
            raise AssertionError(
                f"required-ci.yml: Required CI does not need {job_id}"
            )

    if "- conflict_marker" in required_ci or "conflict-marker-check.yml" in required_ci:
        raise AssertionError(
            "required-ci.yml: conflict-marker-check must stay outside the aggregate"
        )

    print(
        f"Verified parent fan-out for {len(SUITE_WORKFLOWS)} suites and "
        "static Required CI name"
    )


def verify_suites():
    """Verify reusable suites keep workflow_call + required_ci and push."""
    required_keys = []
    for job_id, workflow_name in SUITE_WORKFLOWS.items():
        source = (WORKFLOW_DIR / workflow_name).read_text(encoding="utf-8")
        for fragment in ("workflow_call:", "required_ci:", "type: boolean"):
            if fragment not in source:
                raise AssertionError(
                    f"{workflow_name}: missing reusable contract {fragment!r}"
                )
        if re.search(r"(?m)^  pull_request:$", source):
            raise AssertionError(
                f"{workflow_name}: parent must be the only suite PR listener"
            )
        if not re.search(r"(?m)^  push:$", source):
            raise AssertionError(f"{workflow_name}: existing push trigger was removed")

        has_dispatch = re.search(r"(?m)^  workflow_dispatch:$", source) is not None
        check_equal(
            f"{workflow_name} workflow_dispatch preservation",
            has_dispatch,
            workflow_name in WORKFLOW_DISPATCH_SUITES,
        )

        if job_id == "web_ui":
            if "'web/web/**'" not in source and "web/web/**" not in source:
                raise AssertionError("web-ui-tests.yml: missing push path filter")
            continue

        expected_key = REQUIRED_CONCURRENCY_KEYS[job_id]
        group = re.search(r"(?m)^  group: (.+)$", source)
        if not group:
            raise AssertionError(f"{workflow_name}: missing concurrency group")
        if expected_key not in group.group(1):
            raise AssertionError(
                f"{workflow_name}: missing unique required concurrency key "
                f"{expected_key!r}"
            )
        required_keys.append(expected_key)
        if not re.search(r"(?m)^  cancel-in-progress: true$", source):
            raise AssertionError(f"{workflow_name}: missing cancel-in-progress: true")

    check_equal(
        "required concurrency key uniqueness",
        len(set(required_keys)),
        len(REQUIRED_CONCURRENCY_KEYS),
    )
    print(f"Verified {len(SUITE_WORKFLOWS)} reusable suite contracts")


def verify_conflict_marker_standalone():
    """Verify conflict-marker-check remains a standalone required-check candidate."""
    source = CONFLICT_MARKER_WORKFLOW.read_text(encoding="utf-8")
    if not re.search(r"(?m)^  pull_request:$", source):
        raise AssertionError("conflict-marker-check.yml: missing pull_request trigger")
    job = job_block(source, "conflict-marker-check")
    check_equal(
        "conflict-marker-check name",
        static_job_name(job),
        "conflict-marker-check",
    )
    print("Verified standalone conflict-marker-check")


def main():
    """Run the Required CI source contract checks."""
    simulate_aggregate()
    verify_parent()
    verify_suites()
    verify_conflict_marker_standalone()
    print("Required CI contract check passed without starting CI")


if __name__ == "__main__":
    main()
