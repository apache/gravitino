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
function fixture() {
  const repo = { owner: 'apache', repo: 'gravitino' };
  const pr = { number: 7, state: 'open', head: { sha: A, ref: 'topic',
    repo: { owner: { login: 'fork' }, name: 'repo', full_name: 'fork/repo' } } };
  const run = { id: 10, run_attempt: 1, head_sha: B, head_branch: 'topic', event: 'push',
    path: '.github/workflows/fork-ci.yml', status: 'completed', conclusion: 'cancelled' };
  const record = { owner: 'fork', repo: 'repo', run_id: 10, run_attempt: 1, trusted: false };
  const check = { id: 20, app: { slug: 'github-actions' }, status: 'in_progress',
    output: { text: JSON.stringify(record) } };
  const writes = [], statuses = [];
  const actual = { ...run }, current = structuredClone(pr);
  const github = { rest: {
    actions: { listWorkflowRuns: async args => { assert.equal(args.per_page, 10);return { data: { workflow_runs: [run] } }; },
      getWorkflowRunAttempt: async args => { assert.equal(args.attempt_number, 1);return { data: actual }; } },
    checks: { listForRef() {}, update: async args => { writes.push(args);Object.assign(check, args); } },
    pulls: { get: async () => ({ data: current }) },
    repos: { getCombinedStatusForRef: async () => ({ data: { statuses } }),
      createCommitStatus: async args => { statuses.push(args); } }
  }, paginate: async () => [check] };
  return { github, repo, pr, run, record, check, actual, current, writes, statuses };
}
test('confirmed cancelled previous runs close their metadata check and visible status', async () => {
  const f = fixture();await retireCancelled(f);
  assert.equal(f.writes.length, 2);
  assert.equal(f.writes[0].status, 'completed');
  assert.equal(f.writes[0].conclusion, 'cancelled');
  assert.equal(JSON.parse(f.writes[0].output.text).superseded_by, A);
  assert.deepEqual(f.statuses[0], { owner: 'apache', repo: 'gravitino', sha: B, context: 'Fork CI', state: 'error',
    description: 'Fork CI cancelled for a previous commit', target_url: 'https://github.com/fork/repo/actions/runs/10' });
});
test('history retirement preserves other heads, runs, attempts, trusted results and terminal verdicts', async () => {
  const changes = [
    f => f.run.head_sha = A, f => f.run.head_branch = 'another', f => f.run.event = 'workflow_dispatch',
    f => f.run.path = '.github/workflows/other.yml', f => f.run.conclusion = 'success',
    f => f.record.owner = 'other', f => f.record.repo = 'other', f => f.record.trusted = true,
    f => f.record.run_id = 11, f => f.record.run_attempt = 2,
    f => { f.check.status = 'completed';f.check.conclusion = 'success'; },
    f => f.actual.status = 'in_progress', f => f.actual.conclusion = 'failure', f => f.actual.head_sha = A,
    f => f.current.state = 'closed', f => f.current.head.sha = B,
    f => f.current.head.repo.full_name = 'other/repo'
  ];
  for (const change of changes) {
    const f = fixture();change(f);f.check.output.text = JSON.stringify(f.record);
    await retireCancelled(f);assert.equal(f.writes.length, 0);assert.equal(f.statuses.length, 0);
  }
  const f = fixture();f.check.output.text = 'invalid JSON';await retireCancelled(f);assert.equal(f.writes.length, 0);
});
test('retirement retries a missing status after its metadata write succeeded', async () => {
  const f = fixture(), create = f.github.rest.repos.createCommitStatus;
  f.github.rest.repos.createCommitStatus = async () => { throw new Error('Status permission failure'); };
  await assert.rejects(retireCancelled(f), /Status permission failure/);
  assert.equal(f.check.status, 'completed');assert.equal(f.statuses.length, 0);
  f.github.rest.repos.createCommitStatus = create;await retireCancelled(f);
  assert.equal(f.writes.length, 2);assert.equal(f.statuses.length, 1);assert.equal(f.statuses[0].state, 'error');
});

test('settled retired attempts do not repeatedly query the attempt, PR or status', async () => {
  const f = fixture();await retireCancelled(f);
  assert.equal(JSON.parse(f.check.output.text).retired_status_published, true);
  const unexpected = async () => { throw new Error('Settled history must not be fetched again'); };
  f.github.rest.actions.getWorkflowRunAttempt = unexpected;
  f.github.rest.pulls.get = unexpected;f.github.rest.repos.getCombinedStatusForRef = unexpected;
  await retireCancelled(f);
  assert.equal(f.writes.length, 2);assert.equal(f.statuses.length, 1);
});

test('a fork without the Fork CI workflow has no history to retire', async () => {
  const { github, repo, pr, writes, statuses } = fixture();
  github.rest.actions.listWorkflowRuns = async () => { throw Object.assign(new Error('Not Found'), { status: 404 }); };
  await retireCancelled({ github, repo, pr });
  assert.equal(writes.length, 0); assert.equal(statuses.length, 0);
  github.rest.actions.listWorkflowRuns = async () => { throw Object.assign(new Error('Server Error'), { status: 500 }); };
  await assert.rejects(retireCancelled({ github, repo, pr }), /Server Error/);
});
