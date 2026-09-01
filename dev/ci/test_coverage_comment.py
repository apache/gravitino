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

"""Unit tests for the coverage-comment decision model.

These tests do not contact GitHub. They lock the skip/post rules Yuqi asked
for on PR 12545 (cancelled runs, no fork PR-number artifact, live head match)
and the SHA stamp shared with jacoco_report.py.
"""

import sys
from pathlib import Path


CI_DIR = Path(__file__).resolve().parent
if str(CI_DIR) not in sys.path:
    sys.path.insert(0, str(CI_DIR))

import coverage_comment
import jacoco_report


def check_equal(name, actual, expected):
    """Raise a readable error when a value differs from the expected one."""
    if actual != expected:
        raise AssertionError(f"{name}: expected {expected!r}, got {actual!r}")


def matching_run():
    """Return a workflow_run whose head matches ``matching_pull``."""
    return {
        "event": "pull_request",
        "conclusion": "success",
        "head_sha": "abc123",
        "head_branch": "feat/coverage",
        "head_repository": {
            "full_name": "fork/gravitino",
            "owner": {"login": "fork"},
        },
        "pull_requests": [{"number": 12545}],
    }


def matching_pull():
    """Return a live PR whose head matches ``matching_run``."""
    return {
        "number": 12545,
        "head": {
            "sha": "abc123",
            "ref": "feat/coverage",
            "repo": {"full_name": "fork/gravitino"},
        },
    }


def decide_ok(**overrides):
    """Call decide() against matching run/pull, with optional overrides."""
    kwargs = {
        "event": "pull_request",
        "conclusion": "success",
        "has_artifact": True,
        "workflow_run": matching_run(),
        "pull": matching_pull(),
        "pr_number": 12545,
    }
    kwargs.update(overrides)
    return coverage_comment.decide(**kwargs)


def test_cancelled_and_skipped_do_not_post():
    """Superseded Required CI runs must not overwrite the coverage comment."""
    for conclusion in ("cancelled", "skipped"):
        action, reason = decide_ok(conclusion=conclusion)
        check_equal(f"{conclusion} action", action, coverage_comment.ACTION_SKIP)
        check_equal(
            f"{conclusion} reason",
            reason,
            f"Required CI conclusion is {conclusion}",
        )


def test_failed_sibling_suite_still_posts():
    """A red spark/backend run must not hide a report build already uploaded."""
    action, reason = decide_ok(conclusion="failure")
    check_equal("failure action", action, coverage_comment.ACTION_POST)
    check_equal("failure reason", reason, "ok")


def test_missing_artifact_skips():
    """Path-skip or failed-before-upload stays green and posts nothing."""
    action, reason = decide_ok(has_artifact=False)
    check_equal("no artifact action", action, coverage_comment.ACTION_SKIP)
    check_equal(
        "no artifact reason",
        reason,
        "no coverage-report artifact on the Required CI run",
    )


def test_unresolved_pr_skips_and_ignores_artifact_number():
    """A fork-controlled pr-number.txt must not be enough to post."""
    action, reason = decide_ok(pull=None, pr_number=None)
    check_equal("unresolved action", action, coverage_comment.ACTION_SKIP)
    check_equal(
        "unresolved reason",
        reason,
        "could not uniquely resolve PR from workflow_run",
    )
    # Even if a caller passed a number from an artifact, no live pull -> skip.
    action, reason = decide_ok(pull=None, pr_number=99999)
    check_equal("artifact number action", action, coverage_comment.ACTION_SKIP)


def test_stale_head_does_not_post():
    """Live PR head must match workflow_run SHA, repo, and branch."""
    stale_sha = matching_pull()
    stale_sha["head"]["sha"] = "ddd999"
    action, reason = decide_ok(pull=stale_sha)
    check_equal("sha mismatch action", action, coverage_comment.ACTION_SKIP)
    check_equal(
        "sha mismatch reason",
        reason,
        "stale or mismatched PR head SHA/repo/branch",
    )

    stale_branch = matching_pull()
    stale_branch["head"]["ref"] = "other-branch"
    action, _ = decide_ok(pull=stale_branch)
    check_equal("branch mismatch action", action, coverage_comment.ACTION_SKIP)

    stale_repo = matching_pull()
    stale_repo["head"]["repo"]["full_name"] = "evil/gravitino"
    action, _ = decide_ok(pull=stale_repo)
    check_equal("repo mismatch action", action, coverage_comment.ACTION_SKIP)


def test_matching_head_posts():
    """Happy path: live PR head matches the Required CI run."""
    action, reason = decide_ok()
    check_equal("match action", action, coverage_comment.ACTION_POST)
    check_equal("match reason", reason, "ok")


def test_resolve_pr_uses_workflow_run_not_a_file():
    """PR identity comes from workflow_run, never pr-number.txt."""
    run = matching_run()
    number = coverage_comment.resolve_pr_number(run, lambda head: [])
    check_equal("embedded pull number", number, 12545)

    run_no_embedded = matching_run()
    run_no_embedded["pull_requests"] = []
    listed = [{"number": 77}]
    number = coverage_comment.resolve_pr_number(
        run_no_embedded, lambda head: listed if head == "fork:feat/coverage" else []
    )
    check_equal("listed pull number", number, 77)

    number = coverage_comment.resolve_pr_number(
        run_no_embedded, lambda head: [{"number": 1}, {"number": 2}]
    )
    check_equal("ambiguous pulls", number, None)


def test_stamp_commit_matches_jacoco_line():
    """Sidecar and jacoco_report.py must emit the same commit line."""
    sha = "abc123def"
    stamped = coverage_comment.stamp_commit(
        f"{coverage_comment.COMMENT_MARKER}\n### Code Coverage Report\n",
        sha,
    )
    check_equal(
        "stamped body",
        coverage_comment.commit_line(sha) in stamped,
        True,
    )
    check_equal("idempotent stamp", coverage_comment.stamp_commit(stamped, sha), stamped)


def test_jacoco_report_uses_the_same_marker_and_sha_line():
    """The markdown artifact must already carry the sidecar's marker and SHA."""
    report, _, _ = jacoco_report.generate_report(
        {"LINE": {"missed": 1, "covered": 9}},
        {},
        {},
        [],
        40,
        60,
        ":green_circle:",
        ":red_circle:",
        "abc123def",
        "https://github.com/apache/gravitino",
    )
    check_equal("jacoco marker", coverage_comment.COMMENT_MARKER in report, True)
    check_equal(
        "jacoco commit line",
        coverage_comment.commit_line("abc123def") in report,
        True,
    )


class FakeApi:
    """In-memory GitHub stand-in for create-vs-update tests."""

    def __init__(self, comments=None, pull=None, pulls=None):
        self.comments = list(comments or [])
        self.pull = pull
        self.pulls = list(pulls or [])
        self.created = []
        self.updated = []

    def list_pulls(self, head):
        return self.pulls

    def get_pull(self, number):
        if self.pull is not None and self.pull.get("number") == number:
            return self.pull
        return None

    def list_issue_comments(self, number):
        return self.comments

    def create_comment(self, number, body):
        self.created.append((number, body))
        return {"id": 1, "body": body}

    def update_comment(self, comment_id, body):
        self.updated.append((comment_id, body))
        return {"id": comment_id, "body": body}


def test_post_or_update_replaces_existing_coverage_comment():
    """The sidecar updates the previous coverage comment instead of duplicating."""
    api = FakeApi(
        comments=[{"id": 42, "body": f"{coverage_comment.COMMENT_MARKER}\nold"}]
    )
    result = coverage_comment.post_or_update(api, 12545, "new-body")
    check_equal("update result", result, "updated")
    check_equal("updated id", api.updated[0][0], 42)
    check_equal("created count", len(api.created), 0)


def test_post_or_update_creates_when_missing():
    """The first coverage comment on a PR is created, not updated."""
    api = FakeApi(comments=[{"id": 7, "body": "unrelated"}])
    result = coverage_comment.post_or_update(api, 12545, "new-body")
    check_equal("create result", result, "created")
    check_equal("created number", api.created[0][0], 12545)
    check_equal("updated count", len(api.updated), 0)


def workflow_event(run):
    """Wrap a workflow_run dict the way GitHub delivers workflow_run events."""
    return {"workflow_run": run}


def test_process_skips_cancelled_without_writing():
    """A cancelled Required CI run must not create or update a comment."""
    run = matching_run()
    run["conclusion"] = "cancelled"
    api = FakeApi(pull=matching_pull())
    action, reason, result = coverage_comment.process(
        workflow_event(run), True, "body", api
    )
    check_equal("cancelled process action", action, coverage_comment.ACTION_SKIP)
    check_equal("cancelled process result", result, None)
    check_equal("cancelled created", len(api.created), 0)
    check_equal("cancelled updated", len(api.updated), 0)


def test_process_posts_when_live_head_matches():
    """Matching live PR head posts a SHA-stamped comment."""
    api = FakeApi(pull=matching_pull())
    marker = coverage_comment.COMMENT_MARKER
    action, reason, result = coverage_comment.process(
        workflow_event(matching_run()), True, f"{marker}\nreport", api
    )
    check_equal("process action", action, coverage_comment.ACTION_POST)
    check_equal("process result", result, "created")
    check_equal("created count", len(api.created), 1)
    check_equal(
        "stamped sha",
        coverage_comment.commit_line("abc123") in api.created[0][1],
        True,
    )


def test_process_ignores_artifact_pr_number_file():
    """process() never reads a PR number from the report body or a sidecar file."""
    api = FakeApi(pull=None, pulls=[])
    run = matching_run()
    run["pull_requests"] = []
    action, _, result = coverage_comment.process(
        workflow_event(run), True, "pr-number.txt says 99999", api
    )
    check_equal("no resolved pr action", action, coverage_comment.ACTION_SKIP)
    check_equal("no resolved pr result", result, None)
    check_equal("no write", len(api.created) + len(api.updated), 0)


def test_workflow_yaml_stays_synced_with_python_model():
    """coverage-comment.yml must call this module and skip the same conclusions."""
    yaml_text = (CI_DIR.parents[1] / ".github" / "workflows" / "coverage-comment.yml").read_text(
        encoding="utf-8"
    )
    check_equal(
        "sidecar entry point",
        "python3 dev/ci/coverage_comment.py" in yaml_text,
        True,
    )
    check_equal("no inline github-script", "github-script" in yaml_text, False)
    check_equal("no pr-number artifact", "pr-number.txt" in yaml_text, False)
    check_equal("checkout default-branch sha", "ref: ${{ github.sha }}" in yaml_text, True)
    for conclusion in coverage_comment.SKIPPED_CONCLUSIONS:
        fragment = f"conclusion != '{conclusion}'"
        check_equal(f"yaml skips {conclusion}", fragment in yaml_text, True)
    jacoco = (CI_DIR / "jacoco_report.py").read_text(encoding="utf-8")
    check_equal(
        "jacoco imports shared marker",
        "from coverage_comment import COMMENT_MARKER, commit_line" in jacoco,
        True,
    )


def main():
    """Run coverage-comment model tests without contacting GitHub."""
    tests = [
        test_cancelled_and_skipped_do_not_post,
        test_failed_sibling_suite_still_posts,
        test_missing_artifact_skips,
        test_unresolved_pr_skips_and_ignores_artifact_number,
        test_stale_head_does_not_post,
        test_matching_head_posts,
        test_resolve_pr_uses_workflow_run_not_a_file,
        test_stamp_commit_matches_jacoco_line,
        test_jacoco_report_uses_the_same_marker_and_sha_line,
        test_post_or_update_replaces_existing_coverage_comment,
        test_post_or_update_creates_when_missing,
        test_process_skips_cancelled_without_writing,
        test_process_posts_when_live_head_matches,
        test_process_ignores_artifact_pr_number_file,
        test_workflow_yaml_stays_synced_with_python_model,
    ]
    for test in tests:
        test()
        print(f"Passed {test.__name__}")
    print(f"Passed {len(tests)} coverage-comment model tests")


if __name__ == "__main__":
    main()
