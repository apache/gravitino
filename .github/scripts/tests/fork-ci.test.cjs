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
const { mkdtemp, readFile, rm } = require('node:fs/promises');
const os = require('node:os');
const path = require('node:path');
const { SUITES, requiredName, evaluate, createService } = require('../fork-ci-status.cjs');
const { resolve } = require('../resolve-fork-ci.cjs');
const { yamlEntries, prepare } = require('../allowlist-pr.cjs');
const A = 'a'.repeat(40), B = 'b'.repeat(40), C = 'c'.repeat(40);
const pr = () => ({ number: 7, state: 'open', head: { sha: A, repo: { owner: { login: 'fork' }, name: 'repo', full_name: 'fork/repo' } },
  base: { ref: 'main', sha: B }, updated_at: '2026-01-01T00:00:00Z', html_url: 'https://github.com/apache/gravitino/pull/7' });
const run = () => ({ id: 1, run_attempt: 1, path: '.github/workflows/fork-ci.yml', head_sha: A,
  status: 'completed', conclusion: 'success', created_at: '2026-01-01T00:00:00Z' });
const jobs = p => [{ name: requiredName(p), conclusion: 'success' },
  ...SUITES.map(suite => ({ name: `${suite} / changes`, conclusion: 'success' }))];

test('a same-named empty Required CI job cannot satisfy the gate', () => {
  assert.equal(evaluate(pr(), run(), [{ name: 'Required CI', conclusion: 'success' }]).conclusion, 'action_required');
});
test('every real caller must have successful jobs, with only path-based skips allowed', () => {
  const p = pr();
  assert.equal(evaluate(p, run(), jobs(p)).conclusion, 'success');
  for (const suite of SUITES) {
    assert.equal(evaluate(p, run(), jobs(p).filter(job => !job.name.startsWith(suite + ' / '))).conclusion, 'action_required');
  }
  const children = [...jobs(p), { name: 'build / coverage', conclusion: 'skipped' }];
  assert.equal(evaluate(p, run(), children).conclusion, 'success');
  children.at(-1).conclusion = 'failure';
  assert.equal(evaluate(p, run(), children).conclusion, 'action_required');
});
test('the head, base branch and base SHA must all match the tested context', () => {
  const p = pr(), oldJobs = jobs(p);
  for (const change of [q => q.head.sha = C, q => q.base.sha = C, q => q.base.ref = 'branch-1.3']) {
    const q = pr(); change(q);
    assert.equal(evaluate(q, run(), oldJobs).conclusion, 'action_required');
  }
});
test('running and failed reruns cannot remain successful', () => {
  assert.equal(evaluate(pr(), { ...run(), status: 'in_progress' }, jobs(pr())).status, 'in_progress');
  for (const conclusion of ['failure', 'timed_out', 'cancelled']) {
    assert.equal(evaluate(pr(), { ...run(), conclusion }, jobs(pr())).conclusion, conclusion);
  }
});
// Mirrors Octokit, which returns the workflow_runs array when paginating listWorkflowRuns.
const runsOf = (github, endpoint, args) => endpoint === github.rest.actions.listWorkflowRuns
  ? endpoint(args).then(response => response.data.workflow_runs) : undefined;
function serviceMock({ changed = false, trusted = false, forkJobs = jobs(pr()), trustedJobs = jobs(pr()), trustedSha = B } = {}) {
  const github = { rest: {
    git: { getTree: async ({ owner, tree_sha }) => ({ data: { tree: tree_sha.length === 40
      ? [{ path: '.github', sha: 'github-tree' }, { path: '.asf.yaml', sha: 'policy' }]
      : ['workflows', 'actions', 'scripts'].map(name => ({ path: name, sha: changed && owner === 'fork' ? 'changed-' + name : name })) } }) },
    actions: { listWorkflowRuns: async ({ event }) => ({ data: { workflow_runs: event === 'push' ? [run()] : trusted
      ? [{ ...run(), id: 2, head_sha: trustedSha, display_title: 'Trusted Fork CI #7', created_at: '2026-01-02T00:00:00Z' }] : [] } }) }
  } };
  github.paginate = async (endpoint, args) => runsOf(github, endpoint, args) ?? (args.run_id === 2 ? trustedJobs : forkJobs);
  return github;
}
test('changed workflow definitions cannot self-attest a successful fork run', async () => {
  const service = createService(serviceMock({ changed: true }));
  assert.equal((await service.mirror(pr())).result.conclusion, 'action_required');
});
test('unchanged definitions and all caller jobs permit a normal fork success', async () => {
  assert.equal((await createService(serviceMock()).mirror(pr())).result.conclusion, 'success');
});
test('only a trusted default-branch run for the exact PR and current base covers CI edits', async () => {
  const result = await createService(serviceMock({ changed: true, trusted: true })).mirror(pr());
  assert.equal(result.result.conclusion, 'success'); assert.equal(result.trusted, true);
  assert.equal((await createService(serviceMock({ changed: true, trusted: true, trustedSha: C })).mirror(pr())).result.conclusion, 'action_required');
});
test('a trusted rerun failure overrides an earlier fork success', async () => {
  const github = serviceMock({ trusted: true });
  const list = github.rest.actions.listWorkflowRuns;
  github.rest.actions.listWorkflowRuns = async args => {
    const response = await list(args);
    if (args.event === 'workflow_dispatch') response.data.workflow_runs[0].conclusion = 'failure';
    return response;
  };
  assert.equal((await createService(github).mirror(pr())).result.conclusion, 'failure');
});

function context(repository, fork = false) {
  const [owner, repo] = repository.split('/');
  return { repo: { owner, repo }, eventName: 'push', ref: 'refs/heads/topic', sha: A,
    payload: { repository: { fork, default_branch: 'main' } } };
}
const coreFor = output => ({ setOutput: (key, value) => output[key] = value, notice: () => {} });
test('standalone repositories run their own commit without Apache sync', async () => {
  const output = {};
  await resolve({ context: context('copy/repo'), core: coreFor(output),
    github: { rest: { repos: { get: async () => ({ data: { fork: false } }) } } } });
  assert.equal(output.valid, 'true'); assert.equal(output.sync, 'false'); assert.equal(output['base-sha'], A);
});
test('fork pushes resolve the PR base once', async () => {
  const output = {};
  await resolve({ context: context('fork/repo', true), core: coreFor(output), github: {
    rest: { pulls: { list() {} }, repos: { get: async () => ({ data: { fork: true, source: { full_name: 'apache/gravitino' } } }),
      getBranch: async () => ({ data: { commit: { sha: B } } }) } }, paginate: async () => [pr()] } });
  assert.equal(output['base-sha'], B); assert.equal(output['base-ref'], 'main'); assert.equal(output.sync, 'true');
});
test('pre-PR base discovery paginates branches, including later release branches', async () => {
  const output = {}; let paginated = false;
  const listBranches = () => {};
  await resolve({ context: context('fork/repo', true), core: coreFor(output), github: {
    rest: { pulls: { list() {} }, repos: { listBranches,
      get: async args => ({ data: args.owner === 'fork' ? { fork: true, source: { full_name: 'apache/gravitino' } } : { default_branch: 'main' } }),
      compareCommitsWithBasehead: async args => ({ data: { ahead_by: args.basehead.startsWith('branch-9.9...') ? 1 : 99 } }),
      getBranch: async () => ({ data: { commit: { sha: B } } }) } },
    paginate: async endpoint => { if (endpoint === listBranches) { paginated = true; return [{ name: 'branch-9.9' }]; } return []; }
  } });
  assert.equal(paginated, true); assert.equal(output['base-ref'], 'branch-9.9');
});
test('trusted dispatch rejects an arbitrary ref or a non-upstream repository', async () => {
  const ctx = context('fork/repo');ctx.eventName = 'workflow_dispatch';ctx.payload.inputs = { 'pull-request': '7' };
  await assert.rejects(resolve({ context: ctx, core: coreFor({}), github: {} }), /apache/);
  const upstream = context('apache/gravitino');upstream.eventName = 'workflow_dispatch';upstream.payload.inputs = { 'pull-request': '7' };
  await assert.rejects(resolve({ context: upstream, core: coreFor({}), github: { rest: { pulls: { get: async () => ({ data: pr() }) } } } }), /current default branch/);
});

test('allowlist scan rejects paths, symlinks, oversized files and truncated listings', () => {
  const file = { path: 'workflows/build.yml', mode: '100644', type: 'blob', size: 10 };
  assert.equal(yamlEntries({ tree: [file] }).length, 1);
  for (const entry of [{ ...file, path: '../escape.yml' }, { ...file, mode: '120000' }, { ...file, size: 2000000 }]) {
    assert.throws(() => yamlEntries({ tree: [entry] }));
  }
  assert.throws(() => yamlEntries({ tree: [file], truncated: true }));
});
test('allowlist downloads only immutable YAML data outside the trusted checkout', async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), 'fork-ci-allowlist-test-'));
  const output = {};
  try {
    const ctx = context('apache/gravitino');ctx.eventName = 'workflow_dispatch';ctx.payload.inputs = { 'pull-request': '7' };
    await prepare({ context: ctx, core: coreFor(output), directory,
      github: { rest: { pulls: { get: async () => ({ data: pr() }) }, git: { getTree: async args => ({ data: { tree: args.tree_sha === A
        ? [{ path: '.github', type: 'tree', sha: 'github' }]
        : [{ path: 'workflows/build.yml', mode: '100644', type: 'blob', size: 8 }, { path: 'scripts/danger.sh', type: 'blob' }] } }) } } },
      fetchImpl: async url => { assert.ok(url.includes('/' + A + '/')); return { ok: true, body: [Buffer.from('on: push')] }; }
    });
    assert.equal(await readFile(path.join(directory, '.github/workflows/build.yml'), 'utf8'), 'on: push');
    assert.equal(JSON.parse(output.prs)[0].sha, A);
    await assert.rejects(readFile(path.join(directory, '.github/scripts/danger.sh')));
  } finally { await rm(directory, { recursive: true, force: true }); }
});

test('a newer fork rerun failure is not hidden by an older trusted success', async () => {
  const github = serviceMock({ trusted: true });
  const list = github.rest.actions.listWorkflowRuns;
  github.rest.actions.listWorkflowRuns = async args => {
    const response = await list(args);
    if (args.event === 'push') Object.assign(response.data.workflow_runs[0], { conclusion: 'failure', updated_at: '2026-01-03T00:00:00Z' });
    return response;
  };
  assert.equal((await createService(github).mirror(pr())).result.conclusion, 'failure');
});

test('all reusable calls and checkout actions forward the frozen context', () => {
  const fs = require('node:fs');
  const directory = path.resolve(__dirname, '../../workflows');
  const visited = new Set();
  const fields = ['base-ref', 'base-sha', 'head-repository', 'head-sha', 'sync'];
  function visit(name) {
    if (visited.has(name)) return;
    visited.add(name);
    const source = fs.readFileSync(path.join(directory, name), 'utf8');
    if (name !== 'fork-ci.yml') for (const field of fields) assert.ok(source.includes(`      ${field}:`), `${name} accepts ${field}`);
    const lines = source.split('\n');
    for (let i = 0; i < lines.length; i++) {
      const match = lines[i].match(/uses: \.\/\.github\/(workflows\/([^\s]+)|actions\/checkout-and-sync)/);
      if (!match) continue;
      const indent = lines[i].match(/^ */)[0].length;
      let block = '';
      if (match[2] && name === 'fork-ci.yml') {
        block = lines.slice(Math.max(0, i - 7), i).join('\n');
      } else {
        for (let j = i + 1; j < lines.length && (!lines[j].trim() || lines[j].match(/^ */)[0].length >= indent); j++) {
          if (lines[j].match(/^ */)[0].length === indent && lines[j].trim().startsWith('- ')) break;
          block += lines[j] + '\n';
        }
      }
      for (const field of fields) assert.ok(block.includes(`${field}: $` + '{{'), `${name} forwards ${field}`);
      if (match[2]) visit(match[2]);
    }
  }
  visit('fork-ci.yml');
  assert.ok(visited.size > 20, 'nested matrix workflows are included');
});

test('immutable CI definition comparisons are cached by repository, head and base', async () => {
  const github = serviceMock(); let calls = 0;
  const get = github.rest.git.getTree;
  github.rest.git.getTree = async args => { calls++; return get(args); };
  const service = createService(github);
  assert.equal(await service.unchanged(pr()), true);
  assert.equal(calls, 4);
  assert.equal(await service.unchanged(pr()), true);
  assert.equal(calls, 4);
  const changedBase = pr(); changedBase.base.sha = C;
  await service.unchanged(changedBase);
  assert.equal(calls, 6);
  assert.equal(await createService(github).unchanged(pr()), true);
  assert.equal(calls, 10);
});

const { update } = require('../update-fork-ci.cjs');
function publisherMock({ current = pr(), latest, rerun = run(), failTrees = false } = {}) {
  const github = serviceMock();
  const writes = [];
  const counters = { runs: 0 };
  const listForRef = () => {};
  const listPulls = () => {};
  let reads = 0;
  github.rest.pulls = { list: listPulls, get: async () => ({ data: ++reads === 1 ? pr() : current }) };
  github.rest.repos = { getContent: async () => ({ data: {} }) };
  github.rest.rateLimit = { get: async () => ({ data: { resources: { core: { remaining: 900, limit: 1000 } } } }) };
  github.rest.checks = { listForRef,
    create: async args => { writes.push({ create: args }); },
    update: async args => { writes.push({ update: args }); } };
  github.rest.actions.listWorkflowRuns = async args => {
    counters.runs++;
    return { data: { workflow_runs: args.event === 'push' ? [rerun] : [] } };
  };
  if (failTrees) github.rest.git.getTree = async () => { throw new Error('API unavailable'); };
  github.paginate = async (endpoint, args) => runsOf(github, endpoint, args) ?? (endpoint === listPulls ? [pr()]
    : endpoint === listForRef ? (latest ? [latest] : []) : jobs(pr()));
  github.hook = { before() {} };
  return { github, writes, counters };
}
async function withPublisher(fn) {
  const directory = await mkdtemp(path.join(os.tmpdir(), 'fork-ci-publisher-test-'));
  const stateFile = path.join(directory, 'state.json');
  const ctx = context('apache/gravitino');ctx.eventName = 'workflow_dispatch';ctx.payload.inputs = { 'pull-request': '7' };
  const failures = [];
  const core = { info() {}, warning() {}, setFailed: message => failures.push(message) };
  try { await fn({ ctx, stateFile, core, failures }); }
  finally { await rm(directory, { recursive: true, force: true }); }
}
test('publisher binds the check to the exact base and current run attempt', async () => withPublisher(async ({ ctx, stateFile, core }) => {
  const { github, writes } = publisherMock();
  await update({ github, context: ctx, core, stateFile, sleep: async () => {} });
  assert.equal(writes.length, 1);
  const check = writes[0].create;
  assert.equal(check.conclusion, 'success');assert.equal(check.head_sha, A);
  assert.equal(JSON.parse(check.output.text).base_sha, B);
  assert.equal(JSON.parse(check.output.text).run_attempt, 1);
}));
test('a push or retarget during publication cannot publish stale success', async () => withPublisher(async ({ ctx, stateFile, core }) => {
  const changed = pr(); changed.base.sha = C;
  const { github, writes } = publisherMock({ current: changed });
  await update({ github, context: ctx, core, stateFile, sleep: async () => {} });
  assert.equal(writes.length, 0);
}));
test('manual publication refreshes a cached final success and observes a failed rerun', async () => withPublisher(async ({ ctx, stateFile, core }) => {
  const old = { id: 55, app: { slug: 'github-actions' }, status: 'completed', conclusion: 'success',
    output: { text: JSON.stringify({ run_id: 1, run_attempt: 1, base_sha: B, base_ref: 'main' }) } };
  const fs = require('node:fs/promises');
  await fs.writeFile(stateFile, JSON.stringify({ cursor: 0, prs: { 7: { sha: A, base: B, base_ref: 'main',
    updated_at: pr().updated_at, status: 'completed', checked_at: new Date().toISOString(), completed_at: new Date().toISOString() } } }));
  const { github, writes, counters } = publisherMock({ latest: old, rerun: { ...run(), run_attempt: 2, conclusion: 'failure' } });
  await update({ github, context: ctx, core, stateFile, sleep: async () => {} });
  assert.ok(counters.runs > 0); assert.equal(writes[0].create.conclusion, 'failure');
  assert.equal(JSON.parse(writes[0].create.output.text).run_attempt, 2);
}));
test('publisher failures remain observable and are retried instead of cached', async () => withPublisher(async ({ ctx, stateFile, core, failures }) => {
  const { github, writes } = publisherMock({ failTrees: true });
  await update({ github, context: ctx, core, stateFile, sleep: async () => {} });
  assert.equal(writes.length, 0); assert.equal(failures.length, 1);
  await assert.rejects(readFile(stateFile, 'utf8'), { code: 'ENOENT' });
}));
test('PR signals prioritize their head and bypass settled state', async () => withPublisher(async ({ ctx, stateFile, core }) => {
  ctx.eventName = 'workflow_run';delete ctx.payload.inputs;
  ctx.payload.workflow_run = { name: 'Fork CI PR Signal', head_sha: A, event: 'pull_request' };
  const { github, writes } = publisherMock();
  await update({ github, context: ctx, core, stateFile, sleep: async () => {} });
  assert.equal(writes.length, 1);
}));

test('scheduled publication ignores poisoned cache state and recomputes definitions', async () => withPublisher(async ({ ctx, stateFile, core }) => {
  ctx.eventName = 'schedule'; delete ctx.payload.inputs;
  await require('node:fs/promises').writeFile(stateFile, JSON.stringify({ cursor: 0, prs: { 7: {
    sha: A, base: B, base_ref: 'main', repository: 'fork/repo', definitions_equal: true,
    updated_at: pr().updated_at, status: 'completed', checked_at: new Date().toISOString(), completed_at: new Date().toISOString()
  } } }));
  const { github, writes } = publisherMock();
  github.rest.git = serviceMock({ changed: true }).rest.git;
  await update({ github, context: ctx, core, stateFile, sleep: async () => {} });
  assert.equal(writes[0].create.conclusion, 'action_required');
  const workflow = await readFile(path.join(__dirname, '../../workflows/fork-ci-status-update.yml'), 'utf8');
  assert.ok(!workflow.includes('actions/cache/'));
}));

test('partial reruns retain successful callers but use each job newest result', async () => {
  const p = pr();
  const github = serviceMock();
  github.rest.actions.listWorkflowRuns = async ({ event }) => ({ data: {
    workflow_runs: event === 'push' ? [{ ...run(), run_attempt: 2 }] : []
  } });
  let rerunConclusion = 'success';
  github.paginate = async (endpoint, args) => {
    const runs = runsOf(github, endpoint, args);
    if (runs) return runs;
    assert.equal(endpoint, 'GET /repos/{owner}/{repo}/actions/runs/{run_id}/jobs');
    assert.equal(args.filter, 'all');
    return [
      ...jobs(p).map((job, i) => ({ ...job, id: i + 1, run_attempt: 1 })),
      { name: requiredName(p), id: 100, run_attempt: 2, conclusion: 'success' },
      { name: 'backend / changes', id: 101, run_attempt: 2, conclusion: rerunConclusion }
    ].reverse();
  };
  assert.equal((await createService(github).mirror(p)).result.conclusion, 'success');
  rerunConclusion = 'failure';
  assert.equal((await createService(github).mirror(p)).result.conclusion, 'action_required');
});

test('allowlist signals find fork PRs by head branch', async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), 'fork-ci-allowlist-test-'));
  const output = {};
  try {
    const ctx = context('apache/gravitino');ctx.eventName = 'workflow_run';
    ctx.payload.workflow_run = { head_sha: A, head_branch: 'topic', head_repository: { owner: { login: 'fork' } } };
    const list = () => {};
    let head;
    await prepare({ context: ctx, core: coreFor(output), directory,
      github: { paginate: async (endpoint, args) => { assert.equal(endpoint, list); head = args.head; return [pr(), { ...pr(), number: 8, head: { ...pr().head, sha: C } }]; },
        rest: { pulls: { list }, git: { getTree: async args => ({ data: { tree: args.tree_sha === A
          ? [{ path: '.github', type: 'tree', sha: 'github' }]
          : [{ path: 'workflows/build.yml', mode: '100644', type: 'blob', size: 8 }] } }) } } },
      fetchImpl: async () => ({ ok: true, body: [Buffer.from('on: push')] })
    });
    assert.equal(head, 'fork:topic');
    assert.deepEqual(JSON.parse(output.prs), [{ number: 7, sha: A }]);
  } finally { await rm(directory, { recursive: true, force: true }); }
});

test('a run against a newer commit of the PR base branch is accepted', async () => {
  for (const [status, conclusion] of [['ahead', 'success'], ['identical', 'success'], ['diverged', 'action_required']]) {
    const tested = pr(); tested.base.sha = C;
    const github = serviceMock({ forkJobs: jobs(tested) });
    let basehead;
    github.rest.repos = { compareCommitsWithBasehead: async args => { basehead = args.basehead; return { data: { status } }; } };
    const mirror = await createService(github).mirror(pr());
    assert.equal(mirror.result.conclusion, conclusion);
    assert.equal(basehead, `${C}...main`);
    assert.equal(mirror.base, conclusion === 'success' ? C : B);
  }
});

test('scheduled sweeps probe recently updated PRs first and refresh only running or rerun checks', async () => withPublisher(async ({ ctx, stateFile, core }) => {
  ctx.eventName = 'schedule'; delete ctx.payload.inputs;
  const { github, writes } = publisherMock();
  const recent = new Date().toISOString();
  const old = Array.from({ length: 20 }, (_, i) => ({ ...pr(), number: 100 + i,
    head: { ...pr().head, sha: (100 + i).toString(16).padStart(40, 'd') } }));
  const settled = { ...pr(), number: 8, updated_at: recent, head: { ...pr().head, sha: 'e'.repeat(40) } };
  const failed = { ...pr(), updated_at: recent };
  const all = [...old, settled, failed];
  const listPulls = github.rest.pulls.list;
  const listForRef = github.rest.checks.listForRef;
  const paginate = github.paginate;
  // Any completed result, not only success, is final until the fork run id or attempt changes.
  const check = completed => ({ id: 1, app: { slug: 'github-actions' }, status: completed ? 'completed' : 'in_progress',
    conclusion: completed ? 'cancelled' : null, output: { text: JSON.stringify({ run_id: 1, run_attempt: 1 }) } });
  github.paginate = async (endpoint, args) => endpoint === listPulls ? all
    : endpoint === listForRef ? [check(args.ref === settled.head.sha)] : paginate(endpoint, args);
  github.rest.pulls.get = async ({ pull_number }) => ({ data: all.find(p => p.number === pull_number) });
  const runsFor = [];
  const list = github.rest.actions.listWorkflowRuns;
  github.rest.actions.listWorkflowRuns = async args => {
    runsFor.push(args.head_sha);
    const response = await list(args);
    response.data.workflow_runs = response.data.workflow_runs.map(run => ({ ...run, head_sha: args.head_sha }));
    return response;
  };
  await update({ github, context: ctx, core, stateFile, sleep: async () => {} });
  // The settled check is probed for its fork run once, which is unchanged, so it is not refreshed.
  const written = writes.map(write => write.create || write.update);
  assert.equal(runsFor.filter(sha => sha === settled.head.sha).length, 1);
  assert.equal(written[0].conclusion, 'success');
  assert.match(written[0].output.summary, /^\[See fork CI results\]\(https:\/\/github\.com\/fork\/repo\/actions\/runs\/1\)/);
}));

test('a failed fork run is not summarized as verified', async () => withPublisher(async ({ ctx, stateFile, core }) => {
  const { github, writes } = publisherMock({ rerun: { ...run(), conclusion: 'failure' } });
  await update({ github, context: ctx, core, stateFile, sleep: async () => {} });
  assert.equal(writes[0].create.conclusion, 'failure');
  assert.doesNotMatch(writes[0].create.output.summary, /Verified/);
}));

test('PR signals do not share a concurrency group', async () => {
  const workflow = await readFile(path.join(__dirname, '../../workflows/fork-ci-status-update.yml'), 'utf8');
  assert.match(workflow, /github\.event\.workflow_run\.head_sha \|\| 'sweep'/);
});

test('scheduled sweeps refresh a settled check when its fork run is rerun', async () => withPublisher(async ({ ctx, stateFile, core }) => {
  ctx.eventName = 'schedule'; delete ctx.payload.inputs;
  const green = { id: 55, app: { slug: 'github-actions' }, status: 'completed', conclusion: 'success',
    output: { text: JSON.stringify({ owner: 'fork', repo: 'repo', run_id: 1, run_attempt: 1, fork_run_id: 1, fork_run_attempt: 1 }) } };
  const recent = { ...pr(), updated_at: new Date().toISOString() };
  const { github, writes } = publisherMock({ latest: green, rerun: { ...run(), run_attempt: 2, conclusion: 'failure' } });
  const listPulls = github.rest.pulls.list, paginate = github.paginate;
  github.paginate = async (endpoint, args) => endpoint === listPulls ? [recent] : paginate(endpoint, args);
  github.rest.pulls.get = async () => ({ data: recent });
  await update({ github, context: ctx, core, stateFile, sleep: async () => {} });
  assert.equal(writes.length, 1);
  assert.equal(writes[0].create.conclusion, 'failure');
  assert.equal(JSON.parse(writes[0].create.output.text).fork_run_attempt, 2);
}));

test('a publisher that observed an older attempt does not overwrite a newer result', async () => withPublisher(async ({ ctx, stateFile, core }) => {
  for (const [latest, observed] of [
    [{ status: 'completed', conclusion: 'failure', attempt: 2 }, { conclusion: 'success', run_attempt: 1 }],
    [{ status: 'completed', conclusion: 'success', attempt: 1 }, { status: 'in_progress', conclusion: null, run_attempt: 1 }]]) {
    const check = { id: 55, app: { slug: 'github-actions' }, status: latest.status, conclusion: latest.conclusion,
      output: { text: JSON.stringify({ owner: 'fork', repo: 'repo', run_id: 1, run_attempt: latest.attempt }) } };
    const { github, writes } = publisherMock({ latest: check, rerun: { ...run(), ...observed } });
    await update({ github, context: ctx, core, stateFile, sleep: async () => {} });
    assert.equal(writes.length, 0);
  }
}));

test('fork runs are paginated so an older run cannot win', async () => {
  const github = serviceMock();
  let paginated = false;
  const paginate = github.paginate;
  github.paginate = async (endpoint, args) => {
    if (endpoint === github.rest.actions.listWorkflowRuns) {
      paginated = true;
      return [{ ...run(), id: 1, conclusion: 'success' }, { ...run(), id: 9, conclusion: 'failure' }];
    }
    return paginate(endpoint, args);
  };
  const found = await createService(github).findRun(pr(), false);
  assert.ok(paginated); assert.equal(found.run.id, 9);
});

test('checkout-and-sync does not persist credentials by default', async () => {
  const action = await readFile(path.join(__dirname, '../../actions/checkout-and-sync/action.yml'), 'utf8');
  assert.match(action, /persist-credentials:\n(?:\s+.*\n)*?\s+default: 'false'/);
});

const { watch } = require('../fail-fast.cjs');
function watcherMock(polls) {
  const calls = { cancel: 0, polls: 0 };
  const github = { rest: { actions: { listJobsForWorkflowRunAttempt() {},
    cancelWorkflowRun: async args => { calls.cancel++; calls.run_id = args.run_id; } } },
  paginate: async (endpoint, args) => { calls.attempt = args.attempt_number; return polls[Math.min(calls.polls++, polls.length - 1)]; } };
  const errors = [];
  return { github, calls, errors, context: { repo: { owner: 'fork', repo: 'repo' }, runId: 42 },
    core: { error: message => errors.push(message), info: message => errors.push(message) }, self: 'Stop on first failure', sleep: async () => {} };
}
const job = (name, status, conclusion = null) => ({ name, status, conclusion });

test('the first failed job cancels the rest of the fork run', async () => {
  const mock = watcherMock([
    [job('Stop on first failure', 'in_progress'), job('build / build', 'in_progress')],
    [job('Stop on first failure', 'in_progress'), job('build / build', 'in_progress'),
      job('backend / BackendIT (h2)', 'completed', 'failure')]]);
  assert.deepEqual(await watch(mock), ['backend / BackendIT (h2)']);
  assert.equal(mock.calls.cancel, 1); assert.equal(mock.calls.run_id, 42);
  // Every poll reports progress, so the watcher is never a black box.
  assert.match(mock.errors[0], /0\/1 jobs completed, 0 failed; waiting for build \/ build$/);
  assert.match(mock.errors.at(-1), /Cancelling the run because these jobs failed: backend \/ BackendIT \(h2\)/);
});

test('the watcher ignores itself and exits without cancelling once all jobs pass', async () => {
  process.env.GITHUB_RUN_ATTEMPT = '2';
  try {
    const done = [job('Stop on first failure', 'completed', 'failure'), job('build / build', 'completed', 'success'),
      job('charts / lint-test', 'completed', 'skipped')];
    const mock = watcherMock([[job('build / build', 'in_progress')], done, done]);
    assert.deepEqual(await watch(mock), []);
    assert.equal(mock.calls.cancel, 0); assert.equal(mock.calls.polls, 3); assert.equal(mock.calls.attempt, 2);
    assert.equal(mock.errors.at(-1), 'All jobs completed without a failure.');
  } finally { delete process.env.GITHUB_RUN_ATTEMPT; }
});

test('a run cancelled by fail-fast is reported as the failed job', () => {
  const p = pr();
  const failed = [...jobs(p), { name: 'backend / BackendIT (h2)', conclusion: 'failure' }, { name: 'spark / SparkIT', conclusion: 'cancelled' }];
  const result = evaluate(p, { ...run(), conclusion: 'cancelled' }, failed);
  assert.equal(result.conclusion, 'failure'); assert.equal(result.title, 'Failed: backend / BackendIT (h2)');
  assert.equal(evaluate(p, { ...run(), conclusion: 'cancelled' }, jobs(p)).conclusion, 'cancelled');
});

test('only the fail-fast job can cancel the run', async () => {
  const workflow = await readFile(path.join(__dirname, '../../workflows/fork-ci.yml'), 'utf8');
  assert.equal(workflow.match(/actions: write/g).length, 1);
  assert.match(workflow, /fail_fast:\n(?:.*\n)*?\s+permissions:\n\s+contents: read\n\s+actions: write\n/);
  assert.match(workflow, /^permissions:\n\s+contents: read\n\s+actions: read\n/m);
  // ubuntu-slim cancels jobs after 15 minutes; the watcher must outlive the slowest suite.
  assert.match(workflow, /fail_fast:\n(?:.*\n)*?\s+runs-on: ubuntu-latest\n/);
});

test('scheduled sweeps scale their budget with the hourly quota', async () => withPublisher(async ({ ctx, stateFile, core }) => {
  ctx.eventName = 'schedule'; delete ctx.payload.inputs;
  const probed = async limit => {
    const { github } = publisherMock();
    const recent = Array.from({ length: 60 }, (_, i) => ({ ...pr(), number: 200 + i, updated_at: new Date().toISOString(),
      head: { ...pr().head, sha: (200 + i).toString(16).padStart(40, 'f') } }));
    const listPulls = github.rest.pulls.list, listForRef = github.rest.checks.listForRef, paginate = github.paginate;
    // Count paginated requests, which is what each probe makes.
    let count = () => {};
    github.hook = { before(_, hook) { count = hook; } };
    github.rest.rateLimit.get = async () => ({ data: { resources: { core: { remaining: limit, limit } } } });
    const seen = new Set();
    github.paginate = async (endpoint, args) => {
      count();
      if (endpoint === listPulls) return recent;
      if (endpoint === listForRef) { seen.add(args.ref); return [{ id: 1, app: { slug: 'github-actions' }, status: 'completed',
        conclusion: 'success', output: { text: JSON.stringify({ run_id: 1, run_attempt: 1 }) } }]; }
      return paginate(endpoint, args);
    };
    // Each pass waits two minutes of fake time, which also advances the rotation.
    let t = Date.now(), passes = 0;
    await update({ github, context: ctx, core, stateFile, clock: () => t, sleep: async ms => { t += ms; passes++; } });
    return { seen: seen.size, passes: passes + 1 };
  };
  // A small quota keeps a single pass; a large one keeps polling within the loop window.
  const small = await probed(1000), large = await probed(15000);
  assert.equal(small.passes, 1); assert.ok(small.seen < 60);
  assert.equal(large.passes, 6); assert.equal(large.seen, 60);
}));

test('a missing fork run waits instead of asking the author to act', async () => {
  const github = serviceMock();
  github.rest.actions.listWorkflowRuns = async () => ({ data: { workflow_runs: [] } });
  const { result } = await createService(github).mirror(pr());
  assert.equal(result.status, 'queued'); assert.equal(result.conclusion, undefined);
  assert.match(result.note, /enable Actions in your fork/);
});

test('an incomplete run listing does not replace a recorded run', async () => withPublisher(async ({ ctx, stateFile, core }) => {
  const green = { id: 55, app: { slug: 'github-actions' }, status: 'completed', conclusion: 'success',
    output: { text: JSON.stringify({ owner: 'fork', repo: 'repo', run_id: 1, run_attempt: 1 }) } };
  const { github, writes } = publisherMock({ latest: green });
  github.rest.actions.listWorkflowRuns = async () => ({ data: { workflow_runs: [] } });
  await update({ github, context: ctx, core, stateFile, sleep: async () => {} });
  assert.equal(writes.length, 0);
}));

test('a scheduled sweep mirrors a fork run that finishes during its polling window', async () => withPublisher(async ({ ctx, stateFile, core }) => {
  ctx.eventName = 'schedule'; delete ctx.payload.inputs;
  const recent = { ...pr(), updated_at: new Date().toISOString() };
  const { github, writes } = publisherMock({ rerun: { ...run(), status: 'in_progress', conclusion: null } });
  const listPulls = github.rest.pulls.list, paginate = github.paginate, list = github.rest.actions.listWorkflowRuns;
  let latest = null;
  github.paginate = async (endpoint, args) => endpoint === listPulls ? [recent]
    : endpoint === github.rest.checks.listForRef ? (latest ? [latest] : []) : paginate(endpoint, args);
  github.rest.checks.create = async args => { writes.push({ create: args }); latest = { id: writes.length, app: { slug: 'github-actions' }, ...args }; };
  github.rest.checks.update = async args => { writes.push({ update: args }); Object.assign(latest, args); };
  github.rest.pulls.get = async () => ({ data: recent });
  github.rest.rateLimit.get = async () => ({ data: { resources: { core: { remaining: 15000, limit: 15000 } } } });
  let t = Date.now(), finished = false;
  github.rest.actions.listWorkflowRuns = async args => {
    const response = await list(args);
    if (finished) response.data.workflow_runs = response.data.workflow_runs.map(r => ({ ...r, status: 'completed', conclusion: 'success' }));
    return response;
  };
  await update({ github, context: ctx, core, stateFile, clock: () => t, sleep: async ms => { t += ms; finished = true; } });
  assert.equal((writes[0].create || writes[0].update).status, 'in_progress');
  assert.equal((writes.at(-1).create || writes.at(-1).update).conclusion, 'success');
}));
