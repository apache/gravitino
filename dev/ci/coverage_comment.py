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

"""Coverage-comment decision model used by the sidecar and JaCoCo report.

The sidecar must not post from cancelled Required CI runs or from a
fork-controlled PR number file. This module is the source of truth for:

- when to skip vs post
- how to resolve a PR from workflow_run metadata
- the comment marker and commit SHA line

coverage-comment.yml runs this file. jacoco_report.py stamps the same SHA
line. Tests exercise the functions without contacting GitHub.
"""

import json
import os
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path


COMMENT_MARKER = "<!-- coverage-report -->"
COVERAGE_REPORT_FILE = "coverage-report.md"
SKIPPED_CONCLUSIONS = frozenset({"cancelled", "skipped"})
ACTION_SKIP = "skip"
ACTION_POST = "post"


def commit_line(sha):
    """Return the visible commit line posted on the coverage comment."""
    return f"Commit `{sha}`."


def stamp_commit(body, sha):
    """Ensure the coverage comment body names ``sha``."""
    if not sha:
        return body
    if sha in body:
        return body
    line = commit_line(sha)
    if COMMENT_MARKER in body:
        return body.replace(COMMENT_MARKER, f"{COMMENT_MARKER}\n{line}", 1)
    return f"{COMMENT_MARKER}\n{line}\n{body}"


def run_head(workflow_run):
    """Return the triggering run's head SHA, branch, repo, and owner login."""
    repo = workflow_run.get("head_repository") or {}
    owner = repo.get("owner") or {}
    return {
        "sha": workflow_run.get("head_sha"),
        "branch": workflow_run.get("head_branch"),
        "repo": repo.get("full_name"),
        "owner": owner.get("login"),
    }


def pull_head(pull):
    """Return the live PR head SHA, branch, and repo full name."""
    head = (pull or {}).get("head") or {}
    repo = head.get("repo") or {}
    return {
        "sha": head.get("sha"),
        "branch": head.get("ref"),
        "repo": repo.get("full_name"),
    }


def resolve_pr_number(workflow_run, list_pulls):
    """Resolve a unique PR number from workflow_run. Never read pr-number.txt."""
    embedded = workflow_run.get("pull_requests") or []
    if len(embedded) == 1 and embedded[0].get("number"):
        return embedded[0]["number"]
    head = run_head(workflow_run)
    if not head["owner"] or not head["branch"]:
        return None
    found = list_pulls(f"{head['owner']}:{head['branch']}")
    if len(found) == 1 and found[0].get("number"):
        return found[0]["number"]
    return None


def decide(event, conclusion, has_artifact, workflow_run, pull, pr_number):
    """Return ``(action, reason)`` for posting a coverage comment.

    A red sibling suite (conclusion ``failure``) still posts when the live PR
    head matches the run. Cancelled or skipped runs do not.
    """
    if event != "pull_request":
        return ACTION_SKIP, "not a pull_request Required CI run"
    if conclusion in SKIPPED_CONCLUSIONS:
        return ACTION_SKIP, f"Required CI conclusion is {conclusion}"
    if not has_artifact:
        return ACTION_SKIP, "no coverage-report artifact on the Required CI run"
    if not pr_number or pull is None:
        return ACTION_SKIP, "could not uniquely resolve PR from workflow_run"
    run = run_head(workflow_run)
    live = pull_head(pull)
    if run["sha"] != live["sha"] or run["branch"] != live["branch"] or run["repo"] != live["repo"]:
        return ACTION_SKIP, "stale or mismatched PR head SHA/repo/branch"
    return ACTION_POST, "ok"


def log_decision(action, reason):
    """Print the sidecar skip/post ledger."""
    print(f"ACTION={action}")
    print(f"REASON={reason}")


class GitHubApi:
    """Minimal GitHub REST client for the coverage sidecar."""

    def __init__(self, token, owner, repo):
        self.token = token
        self.owner = owner
        self.repo = repo

    def _request(self, method, path, params=None, body=None):
        url = f"https://api.github.com{path}"
        if params:
            url = f"{url}?{urllib.parse.urlencode(params)}"
        data = None if body is None else json.dumps(body).encode("utf-8")
        request = urllib.request.Request(
            url,
            data=data,
            method=method,
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {self.token}",
                "X-GitHub-Api-Version": "2022-11-28",
                "User-Agent": "gravitino-coverage-comment",
            },
        )
        try:
            with urllib.request.urlopen(request) as response:
                raw = response.read().decode("utf-8")
                return json.loads(raw) if raw else None
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"GitHub API {method} {path} failed: {exc.code} {detail}") from exc

    def list_pulls(self, head):
        """Return open pulls for ``owner:branch``."""
        return self._request(
            "GET",
            f"/repos/{self.owner}/{self.repo}/pulls",
            params={"state": "open", "head": head},
        ) or []

    def get_pull(self, number):
        """Return one pull request."""
        return self._request("GET", f"/repos/{self.owner}/{self.repo}/pulls/{number}")

    def list_issue_comments(self, number):
        """Return issue comments for a pull request."""
        comments = []
        page = 1
        while True:
            batch = self._request(
                "GET",
                f"/repos/{self.owner}/{self.repo}/issues/{number}/comments",
                params={"per_page": 100, "page": page},
            ) or []
            comments.extend(batch)
            if len(batch) < 100:
                return comments
            page += 1

    def create_comment(self, number, body):
        """Create an issue comment on the pull request."""
        return self._request(
            "POST",
            f"/repos/{self.owner}/{self.repo}/issues/{number}/comments",
            body={"body": body},
        )

    def update_comment(self, comment_id, body):
        """Update an existing issue comment."""
        return self._request(
            "PATCH",
            f"/repos/{self.owner}/{self.repo}/issues/comments/{comment_id}",
            body={"body": body},
        )


def post_or_update(api, pr_number, body):
    """Create or update the coverage comment identified by COMMENT_MARKER."""
    comments = api.list_issue_comments(pr_number)
    existing = next((c for c in comments if COMMENT_MARKER in (c.get("body") or "")), None)
    if existing:
        api.update_comment(existing["id"], body)
        return "updated"
    api.create_comment(pr_number, body)
    return "created"


def process(event, has_artifact, report_body, api):
    """Apply skip/post rules and optionally write the coverage comment.

    Returns ``(action, reason, comment_result_or_none)``. Always stays green
    for skip; GitHub writes happen only on ``ACTION_POST``.
    """
    run = event.get("workflow_run") or {}
    pr_number = resolve_pr_number(run, api.list_pulls)
    pull = api.get_pull(pr_number) if pr_number else None
    action, reason = decide(
        event=run.get("event"),
        conclusion=run.get("conclusion"),
        has_artifact=has_artifact,
        workflow_run=run,
        pull=pull,
        pr_number=pr_number,
    )
    log_decision(action, reason)
    if action != ACTION_POST:
        return action, reason, None
    body = stamp_commit(report_body or "", run.get("head_sha"))
    result = post_or_update(api, pr_number, body)
    print(f"COMMENT={result}")
    return action, reason, result


def main():
    """Entry point for coverage-comment.yml. Stay green on skip."""
    event_path = os.environ.get("GITHUB_EVENT_PATH")
    if not event_path:
        raise SystemExit("GITHUB_EVENT_PATH is not set")
    event = json.loads(Path(event_path).read_text(encoding="utf-8"))
    report_path = Path(os.environ.get("COVERAGE_REPORT_FILE", COVERAGE_REPORT_FILE))
    has_artifact = report_path.is_file()
    report_body = report_path.read_text(encoding="utf-8") if has_artifact else ""

    repository = os.environ.get("GITHUB_REPOSITORY", "")
    if "/" not in repository:
        raise SystemExit("GITHUB_REPOSITORY must be owner/repo")
    owner, repo = repository.split("/", 1)
    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
    if not token:
        raise SystemExit("GITHUB_TOKEN is not set")
    process(event, has_artifact, report_body, GitHubApi(token, owner, repo))


if __name__ == "__main__":
    main()
