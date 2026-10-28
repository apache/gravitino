/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
'use strict';
const UPSTREAM = { owner: 'apache', repo: 'gravitino' };

// Commit statuses are the only signal on the PR: check runs created with GITHUB_TOKEN are
// attached to an arbitrary Actions suite, show up as duplicates such as
// "Conflict Marker Check / Fork CI", and can disappear from the PR's latest-check view.
// The status link therefore also records which run and attempt the status mirrors:
// https://github.com/{owner}/{repo}/actions/runs/{id}/attempts/{attempt}, plus
// #fork-run-{id}-{attempt} for a trusted result, naming the fork run seen at that time.
const RUN_URL = /^https:\/\/github\.com\/([^/]+)\/([^/]+)\/actions\/runs\/(\d+)(?:\/attempts\/(\d+))?(?:#fork-run-(\d+)-(\d+))?$/;

function runUrl(owner, repo, run, fork) {
  return `https://github.com/${owner}/${repo}/actions/runs/${run.id}/attempts/${run.run_attempt || 1}` +
    (fork ? `#fork-run-${fork.id}-${fork.run_attempt || 1}` : '');
}

// Returns the run a status links to, or undefined for a status without a run. Links written
// before attempts were recorded have no run_attempt.
function parseRunUrl(url) {
  const match = RUN_URL.exec(url || '');
  if (!match) return undefined;
  const [, owner, repo, id, attempt, forkId, forkAttempt] = match;
  return { owner, repo, run_id: Number(id), run_attempt: attempt && Number(attempt),
    trusted: owner === UPSTREAM.owner && repo === UPSTREAM.repo,
    fork_run_id: forkId && Number(forkId), fork_run_attempt: forkAttempt && Number(forkAttempt) };
}

async function latestStatus({ github, repo, sha, context }) {
  const { data } = await github.rest.repos.getCombinedStatusForRef({ ...repo, ref: sha, per_page: 100 });
  return data.statuses.find(status => status.context === context);
}

// Pass the latest status as previous when it was just read; undefined reads it here.
async function publishStatus({ github, repo, sha, context, state, description, targetUrl, previous }) {
  const params = { ...repo, sha, context, state, description: description.slice(0, 140), target_url: targetUrl };
  if (previous === undefined) previous = await latestStatus({ github, repo, sha, context });
  if (previous?.state === state && previous.description === params.description && previous.target_url === targetUrl) return;
  await github.rest.repos.createCommitStatus(params);
}

// Waiting for a committer is not a failure of the contributor's change.
const TRUSTED_REQUIRED = 'Fork CI passed; trusted verification required';

function statusState(result) {
  if (result.status !== 'completed') return 'pending';
  if (result.title === TRUSTED_REQUIRED) return 'pending';
  if (result.conclusion === 'success') return 'success';
  return result.conclusion === 'failure' || result.conclusion === 'timed_out' ? 'failure' : 'error';
}

// Whether a published status reports a finished execution, including pending trusted verification.
function statusCompleted(status) {
  return status.state !== 'pending' || status.description === TRUSTED_REQUIRED;
}

module.exports = { TRUSTED_REQUIRED, latestStatus, parseRunUrl, publishStatus, runUrl, statusCompleted, statusState };
