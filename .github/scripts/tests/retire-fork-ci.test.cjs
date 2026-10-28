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
const test = require('node:test');
const assert = require('node:assert/strict');
const { retireCancelled } = require('../retire-fork-ci.cjs');
const A = 'a'.repeat(40), B = 'b'.repeat(40);
const url = (id, attempt) => `https://github.com/fork/repo/actions/runs/${id}/attempts/${attempt}`;
function fixture() {
  const repo = { owner: 'apache', repo: 'gravitino' };
  const pr = { number: 7, state: 'open', head: { sha: A, ref: 'topic',
    repo: { owner: { login: 'fork' }, name: 'repo', full_name: 'fork/repo' } } };
  const run = { id: 10, run_attempt: 1, head_sha: B, head_branch: 'topic', event: 'push',
    path: '.github/workflows/fork-ci.yml', status: 'completed', conclusion: 'cancelled' };
  // The previous head's status still waits on the run its push cancelled.
  const shown = { context: 'Fork CI', state: 'pending', description: 'Fork CI is running', target_url: url(10, 1) };
  const statuses = [], reads = [];
  const actual = { ...run }, current = structuredClone(pr);
  const github = { rest: {
    actions: { listWorkflowRuns: async args => { assert.equal(args.per_page, 10);return { data: { workflow_runs: [run] } }; },
      getWorkflowRunAttempt: async args => { assert.equal(args.attempt_number, 1);return { data: actual }; } },
    pulls: { get: async () => ({ data: current }) },
    repos: { getCombinedStatusForRef: async args => { reads.push(args.ref); return { data: { statuses: [shown] } }; },
      createCommitStatus: async args => { statuses.push(args); Object.assign(shown, { state: args.state,
        description: args.description, target_url: args.target_url }); } }
  } };
  return { github, repo, pr, run, shown, actual, current, statuses, reads };
}
test('confirmed cancelled previous runs get an error status on their head', async () => {
  const f = fixture();await retireCancelled(f);
  assert.deepEqual(f.reads, [B]);
  assert.deepEqual(f.statuses, [{ owner: 'apache', repo: 'gravitino', sha: B, context: 'Fork CI', state: 'error',
    description: 'Fork CI cancelled for a previous commit', target_url: url(10, 1) }]);
});
test('history retirement preserves other heads, runs, attempts, trusted results and verdicts', async () => {
  const changes = [
    f => f.run.head_sha = A, f => f.run.head_branch = 'another', f => f.run.event = 'schedule',
    f => f.run.path = '.github/workflows/other.yml', f => f.run.conclusion = 'success',
    f => f.shown.target_url = 'https://github.com/other/repo/actions/runs/10/attempts/1',
    f => f.shown.target_url = 'https://github.com/apache/gravitino/actions/runs/10/attempts/1',
    f => f.shown.target_url = url(11, 1), f => f.shown.target_url = url(10, 2),
    f => f.shown.target_url = 'https://github.com/fork/repo/actions/workflows/fork-ci.yml',
    f => f.shown.state = 'success', f => f.shown.state = 'failure',
    f => f.shown.description = 'Fork CI passed; trusted verification required',
    f => f.shown.context = 'ASF Allowlist Check',
    f => f.actual.status = 'in_progress', f => f.actual.conclusion = 'failure', f => f.actual.head_sha = A,
    f => f.current.state = 'closed', f => f.current.head.sha = B,
    f => f.current.head.repo.full_name = 'other/repo'
  ];
  for (const change of changes) {
    const f = fixture();change(f);
    await retireCancelled(f);assert.equal(f.statuses.length, 0, change.toString());
  }
});
test('statuses linked before attempts were recorded are retired too', async () => {
  const f = fixture();f.shown.target_url = 'https://github.com/fork/repo/actions/runs/10';
  await retireCancelled(f);
  assert.equal(f.statuses[0].state, 'error');assert.equal(f.statuses[0].target_url, url(10, 1));
});
test('a failed retirement is retried by the next publisher', async () => {
  const f = fixture(), create = f.github.rest.repos.createCommitStatus;
  f.github.rest.repos.createCommitStatus = async () => { throw new Error('Status permission failure'); };
  await assert.rejects(retireCancelled(f), /Status permission failure/);
  f.github.rest.repos.createCommitStatus = create;await retireCancelled(f);
  assert.equal(f.statuses.length, 1);assert.equal(f.statuses[0].state, 'error');
});

test('retired statuses are settled with one status read: no attempt, PR or status write', async () => {
  const f = fixture();await retireCancelled(f);
  const unexpected = async () => { throw new Error('Settled history must not be fetched again'); };
  f.github.rest.actions.getWorkflowRunAttempt = unexpected;f.github.rest.pulls.get = unexpected;
  await retireCancelled(f);
  assert.equal(f.statuses.length, 1);assert.deepEqual(f.reads, [B, B]);
});

test('a fork without the Fork CI workflow has no history to retire', async () => {
  const { github, repo, pr, statuses } = fixture();
  github.rest.actions.listWorkflowRuns = async () => { throw Object.assign(new Error('Not Found'), { status: 404 }); };
  await retireCancelled({ github, repo, pr });
  assert.equal(statuses.length, 0);
  github.rest.actions.listWorkflowRuns = async () => { throw Object.assign(new Error('Server Error'), { status: 500 }); };
  await assert.rejects(retireCancelled({ github, repo, pr }), /Server Error/);
});

test('a cancelled run started with Run workflow in the fork is retired like a push run', async () => {
  const f = fixture();f.run.event = 'workflow_dispatch';f.actual.event = 'workflow_dispatch';
  await retireCancelled(f);
  assert.equal(f.statuses[0].state, 'error');
});
