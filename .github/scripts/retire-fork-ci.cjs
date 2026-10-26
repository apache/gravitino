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
const { publishStatus } = require('./publish-ci-status.cjs');
const { FORK_EVENTS } = require('./fork-ci-status.cjs');

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
    const checks = await github.paginate(github.rest.checks.listForRef, {
      ...repo, ref: run.head_sha, check_name: 'Fork CI', filter: 'all', per_page: 100 });
    const latest = checks.filter(check => check.app?.slug === 'github-actions').sort((a, b) => b.id - a.id)[0];
    let record;
    try { record = JSON.parse(latest?.output?.text || '{}'); } catch { continue; }
    // Never replace a trusted result, a newer attempt, or another run's metadata.
    if (!latest || record.owner !== fork.owner || record.repo !== fork.repo || record.trusted ||
        record.run_id !== run.id || record.run_attempt !== run.run_attempt ||
        (latest.status === 'completed' && !record.superseded_by)) continue;
    // A retired attempt is immutable. Remember a successful status write on the trusted
    // check so later sweeps do not keep fetching its attempt, PR and status forever.
    if (latest.status === 'completed' && latest.conclusion === 'cancelled' && record.retired_status_published) continue;
    const { data: actual } = await github.rest.actions.getWorkflowRunAttempt({ ...fork,
      run_id: run.id, attempt_number: run.run_attempt });
    if (actual.head_sha !== run.head_sha || actual.status !== 'completed' || actual.conclusion !== 'cancelled') continue;
    const { data: current } = await github.rest.pulls.get({ ...repo, pull_number: pr.number });
    if (current.state !== 'open' || current.head.sha !== pr.head.sha ||
        current.head.repo?.full_name !== pr.head.repo.full_name) return;
    const title = 'Fork CI cancelled for a previous commit';
    const details = `https://github.com/${fork.owner}/${fork.repo}/actions/runs/${run.id}`;
    const output = { title, summary: `[See fork CI results](${details})\n\n` +
      `This cancelled run tested a previous PR commit. Current head: ${pr.head.sha}.`,
      text: JSON.stringify({ ...record, run_status: 'completed', run_conclusion: 'cancelled', superseded_by: pr.head.sha }) };
    if (latest.status !== 'completed') await github.rest.checks.update({ ...repo,
      check_run_id: latest.id, status: 'completed', conclusion: 'cancelled', output });
    // Retry this independently if a previous write completed the check but lost its status.
    await publishStatus({ github, repo, sha: run.head_sha, context: 'Fork CI',
      state: 'error', description: title, targetUrl: details });
    await github.rest.checks.update({ ...repo, check_run_id: latest.id,
      output: { ...output, text: JSON.stringify({ ...JSON.parse(output.text), retired_status_published: true }) } });
  }
}
module.exports = { retireCancelled };
