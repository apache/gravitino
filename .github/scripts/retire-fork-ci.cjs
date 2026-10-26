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
const { latestStatus, parseRunUrl, publishStatus, runUrl, statusCompleted } = require('./publish-ci-status.cjs');
const { FORK_EVENTS } = require('./fork-ci-status.cjs');

// A push cancels the fork run of the previous head, whose status would otherwise stay pending.
async function retireCancelled({ github, repo, pr }) {
  if (!pr.head.repo || !pr.head.ref) return;
  const fork = { owner: pr.head.repo.owner.login, repo: pr.head.repo.name };
  // Bound history work independently of branch age. Most pushes supersede just one run.
  let data;
  try {
    ({ data } = await github.rest.actions.listWorkflowRuns({ ...fork, workflow_id: 'fork-ci.yml',
      branch: pr.head.ref, per_page: 10 }));
  } catch (error) {
    // The fork never ran fork-ci.yml, so there is no history to retire.
    if (error.status === 404) return;
    throw error;
  }
  for (const run of data.workflow_runs) {
    if (run.head_sha === pr.head.sha || run.head_branch !== pr.head.ref ||
        run.path !== '.github/workflows/fork-ci.yml' || !FORK_EVENTS.includes(run.event) ||
        run.status !== 'completed' || run.conclusion !== 'cancelled') continue;
    const latest = await latestStatus({ github, repo, sha: run.head_sha, context: 'Fork CI' });
    const shown = parseRunUrl(latest?.target_url);
    // Only finish a status still waiting on exactly this fork attempt. A retired status is no
    // longer pending, so later sweeps skip it with this one read. Never replace a trusted
    // result, a verdict, or a newer attempt; links without an attempt predate attempt records.
    if (!latest || statusCompleted(latest) || !shown || shown.owner !== fork.owner || shown.repo !== fork.repo ||
        shown.run_id !== run.id || (shown.run_attempt && shown.run_attempt !== run.run_attempt)) continue;
    const { data: actual } = await github.rest.actions.getWorkflowRunAttempt({ ...fork,
      run_id: run.id, attempt_number: run.run_attempt });
    if (actual.head_sha !== run.head_sha || actual.status !== 'completed' || actual.conclusion !== 'cancelled') continue;
    const { data: current } = await github.rest.pulls.get({ ...repo, pull_number: pr.number });
    if (current.state !== 'open' || current.head.sha !== pr.head.sha ||
        current.head.repo?.full_name !== pr.head.repo.full_name) return;
    await publishStatus({ github, repo, sha: run.head_sha, context: 'Fork CI', state: 'error',
      description: 'Fork CI cancelled for a previous commit', targetUrl: runUrl(fork.owner, fork.repo, run), previous: latest });
  }
}
module.exports = { retireCancelled };
