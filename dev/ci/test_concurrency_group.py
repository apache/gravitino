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

"""Unit tests for Required CI concurrency group evaluation.

These tests do not contact GitHub. They lock the reusable-workflow caller
context that cancelled 15 of 16 suites on PR 12545: ``github.workflow`` is
the parent name, and ``github.event_name`` stays ``pull_request``.
"""

import re
import sys
from pathlib import Path


CI_DIR = Path(__file__).resolve().parent
if str(CI_DIR) not in sys.path:
    sys.path.insert(0, str(CI_DIR))

import concurrency_group
import test_required_ci as contract


# The expression every suite used before the hardcoded-prefix fix. Under the
# caller github context it resolves to one shared group for every child.
LEGACY_GROUP = (
    "${{ github.event_name == 'workflow_call' && inputs.required_ci "
    "&& 'required-build' || github.workflow }}-"
    "${{ github.event.pull_request.number || github.ref }}"
)


def check_equal(name, actual, expected):
    """Raise a readable error when a value differs from the expected one."""
    if actual != expected:
        raise AssertionError(f"{name}: expected {expected!r}, got {actual!r}")


def test_caller_context_is_pull_request_not_workflow_call():
    """Reusable suites inherit the parent's event name and workflow name."""
    context = concurrency_group.required_ci_call_context()
    check_equal("event_name", context["github.event_name"], "pull_request")
    check_equal("workflow", context["github.workflow"], "Required CI")
    check_equal("required_ci", context["inputs.required_ci"], True)


def test_legacy_expression_collapses_under_caller_context():
    """The event_name == workflow_call guard never fires on a PR call."""
    resolved = concurrency_group.eval_group(
        LEGACY_GROUP, concurrency_group.required_ci_call_context(12545)
    )
    check_equal("legacy caller group", resolved, "Required CI-12545")


def test_legacy_expression_would_collide_across_suites():
    """Sixteen copies of the legacy template share one cancel-in-progress slot."""
    context = concurrency_group.required_ci_call_context(12545)
    groups = {
        job_id: concurrency_group.eval_group(LEGACY_GROUP, context)
        for job_id in contract.SUITE_WORKFLOWS
    }
    clobbered = concurrency_group.collisions(groups)
    check_equal("legacy collision count", len(clobbered), 1)
    check_equal(
        "legacy shared group",
        next(iter(clobbered)),
        "Required CI-12545",
    )
    check_equal(
        "legacy suites in the shared group",
        len(next(iter(clobbered.values()))),
        len(contract.SUITE_WORKFLOWS),
    )


def test_legacy_expression_keeps_push_distinct_from_parent():
    """On push, github.workflow is the child name, so the old key was unique."""
    resolved = concurrency_group.eval_group(
        LEGACY_GROUP, concurrency_group.push_context("build")
    )
    check_equal("legacy push group", resolved, "build-refs/heads/main")


def test_hardcoded_prefix_stays_unique_when_called():
    """A compile-time suite id does not depend on the caller workflow name."""
    template = (
        "required-build-${{ github.event.pull_request.number || github.ref }}"
    )
    check_equal(
        "called",
        concurrency_group.eval_group(
            template, concurrency_group.required_ci_call_context(12545)
        ),
        "required-build-12545",
    )
    check_equal(
        "push",
        concurrency_group.eval_group(
            template, concurrency_group.push_context("build")
        ),
        "required-build-refs/heads/main",
    )


def test_checked_in_suites_do_not_share_a_caller_group():
    """Live YAML must evaluate to one group per suite when Required CI fans out."""
    context = concurrency_group.required_ci_call_context(12545)
    groups = {}
    for job_id, workflow_name in contract.SUITE_WORKFLOWS.items():
        source = (contract.WORKFLOW_DIR / workflow_name).read_text(encoding="utf-8")
        template = concurrency_group.extract_group(source)
        expected = contract.REQUIRED_CONCURRENCY_KEYS[job_id]
        if expected not in template:
            raise AssertionError(
                f"{workflow_name}: missing hardcoded suite id {expected!r}"
            )
        if "github.workflow" in template:
            raise AssertionError(
                f"{workflow_name}: github.workflow is the caller name under "
                "workflow_call; hardcode the suite id"
            )
        if "workflow_call" in template:
            raise AssertionError(
                f"{workflow_name}: event_name is never workflow_call in a callee"
            )
        groups[job_id] = concurrency_group.eval_group(template, context)
        check_equal(
            f"{job_id} called group",
            groups[job_id],
            f"{expected}-12545",
        )
    parent = concurrency_group.extract_group(
        contract.REQUIRED_CI_WORKFLOW.read_text(encoding="utf-8")
    )
    groups["parent"] = concurrency_group.eval_group(parent, context)
    check_equal("parent group", groups["parent"], "required-ci-12545")
    concurrency_group.assert_unique("Required CI fan-out", groups)


def test_checked_in_suites_stay_unique_on_push():
    """Branch pushes must not share a group across different suite files."""
    groups = {}
    for job_id, workflow_name in contract.SUITE_WORKFLOWS.items():
        source = (contract.WORKFLOW_DIR / workflow_name).read_text(encoding="utf-8")
        template = concurrency_group.extract_group(source)
        workflow_title = workflow_display_name(source) or job_id
        groups[job_id] = concurrency_group.eval_group(
            template, concurrency_group.push_context(workflow_title)
        )
    concurrency_group.assert_unique("push", groups)


def workflow_display_name(source):
    """Return the YAML ``name:`` value when it is a static string."""
    match = re.search(r"(?m)^name: (.+)$", source)
    if not match:
        return None
    return match.group(1).strip().strip('"').strip("'")


def main():
    """Run concurrency-group model tests."""
    tests = [
        test_caller_context_is_pull_request_not_workflow_call,
        test_legacy_expression_collapses_under_caller_context,
        test_legacy_expression_would_collide_across_suites,
        test_legacy_expression_keeps_push_distinct_from_parent,
        test_hardcoded_prefix_stays_unique_when_called,
        test_checked_in_suites_do_not_share_a_caller_group,
        test_checked_in_suites_stay_unique_on_push,
    ]
    for test in tests:
        test()
        print(f"Passed {test.__name__}")
    print(f"Passed {len(tests)} concurrency-group cases")


if __name__ == "__main__":
    main()
