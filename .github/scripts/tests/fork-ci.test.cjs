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
const { FAIL_FAST, SUITES, requiredName, evaluate, createService } = require('../fork-ci-status.cjs');
const { resolve } = require('../resolve-fork-ci.cjs');
const { yamlEntries, prepare } = require('../allowlist-pr.cjs');
const A = 'a'.repeat(40), B = 'b'.repeat(40), C = 'c'.repeat(40);
const pr = () => ({ number: 7, state: 'open', head: { sha: A, repo: { owner: { login: 'fork' }, name: 'repo', full_name: 'fork/repo' } },
  base: { ref: 'main', sha: B }, updated_at: '2026-01-01T00:00:00Z', html_url: 'https://github.com/apache/gravitino/pull/7' });
const run = () => ({ id: 1, run_attempt: 1, path: '.github/workflows/fork-ci.yml', head_sha: A, event: 'push',
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
    actions: { listWorkflowRuns: async ({ event }) => ({ data: { workflow_runs: event !== 'workflow_dispatch' ? [run()] : trusted
      ? [{ ...run(), id: 2, head_sha: trustedSha, head_branch: 'main', display_title: 'Trusted Fork CI #7', created_at: '2026-01-02T00:00:00Z' }] : [] } }) }
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

test('trusted dispatch resolves the current default branch after the PR base snapshot ages', async () => {
  const ctx = context('apache/gravitino');ctx.eventName = 'workflow_dispatch';
  ctx.ref = 'refs/heads/main';ctx.sha = C;ctx.payload.inputs = { 'pull-request': '7' };
  const output = {};
  const github = { rest: { pulls: { get: async () => ({ data: pr() }) },
    repos: { getBranch: async () => ({ data: { commit: { sha: C } } }) } } };
  await resolve({ context: ctx, core: coreFor(output), github });
  assert.equal(output['base-sha'], C);
  assert.equal(output['head-sha'], A);
  ctx.sha = B;
  await assert.rejects(resolve({ context: ctx, core: coreFor({}), github }), /current default branch/);
});

test('trusted runs on an advanced base are found by branch and verified against their actual tested SHA', async () => {
  for (const comparison of ['ahead', 'identical', 'diverged']) {
    const tested = pr();tested.base.sha = C;
    const github = serviceMock({ changed: true, trusted: true, trustedSha: C, trustedJobs: jobs(tested) });
    github.rest.repos = { compareCommitsWithBasehead: async args => {
      assert.equal(args.basehead, `${C}...main`);return { data: { status: comparison } };
    } };
    const list = github.rest.actions.listWorkflowRuns;
    github.rest.actions.listWorkflowRuns = async args => {
      if (args.event === 'workflow_dispatch') { assert.equal(args.branch, 'main');assert.equal(args.head_sha, undefined); }
      return list(args);
    };
    const mirrored = await createService(github).mirror(pr());
    assert.equal(mirrored.base, C);
    assert.equal(mirrored.result.conclusion, comparison === 'diverged' ? 'action_required' : 'success');
  }
});

test('a trusted result for a previous contributor head cannot hide current fork progress', async () => {
  const previous = pr();previous.head.sha = C;
  for (const trustedStatus of ['completed', 'in_progress']) {
    const github = serviceMock({ changed: true, trusted: true, trustedJobs: jobs(previous) });
    const list = github.rest.actions.listWorkflowRuns;
    github.rest.actions.listWorkflowRuns = async args => {
      const response = await list(args);
      Object.assign(response.data.workflow_runs[0], args.event !== 'workflow_dispatch'
        ? { status: 'in_progress', conclusion: null } : { status: trustedStatus });
      return response;
    };
    const mirrored = await createService(github).mirror(pr());
    assert.equal(mirrored.trusted, false);
    assert.equal(mirrored.result.status, 'in_progress');
    assert.equal(mirrored.result.conclusion, undefined);
  }
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
    if (args.event !== 'workflow_dispatch') Object.assign(response.data.workflow_runs[0], { conclusion: 'failure', updated_at: '2026-01-03T00:00:00Z' });
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
// The status link records the fork run and attempt it mirrors.
const url = (id = 1, attempt = 1, owner = 'fork', repo = 'repo') =>
  `https://github.com/${owner}/${repo}/actions/runs/${id}/attempts/${attempt}`;
const forkStatus = (state, description, target_url = url()) => ({ context: 'Fork CI', state, description, target_url });
function publisherMock({ current = pr(), latest, rerun = run(), failTrees = false } = {}) {
  const github = serviceMock();
  const counters = { runs: 0 };
  // Every status written, and the statuses GitHub returns per commit (newest first per context).
  const statuses = [];
  const stored = new Map([[A, latest ? [{ ...latest }] : []]]);
  const listPulls = () => {};
  let reads = 0;
  github.rest.pulls = { list: listPulls, get: async () => ({ data: ++reads === 1 ? pr() : current }) };
  github.rest.repos = { getContent: async () => ({ data: {} }),
    getCombinedStatusForRef: async ({ ref }) => ({ data: { statuses: stored.get(ref) || [] } }),
    createCommitStatus: async args => { statuses.push(args);
      stored.set(args.sha, [{ ...args }, ...(stored.get(args.sha) || []).filter(s => s.context !== args.context)]); } };
  github.rest.rateLimit = { get: async () => ({ data: { resources: { core: { remaining: 900, limit: 1000 } } } }) };
  github.rest.actions.listWorkflowRuns = async args => {
    counters.runs++;
    return { data: { workflow_runs: args.event !== 'workflow_dispatch' ? [rerun] : [] } };
  };
  if (failTrees) github.rest.git.getTree = async () => { throw new Error('API unavailable'); };
  github.paginate = async (endpoint, args) => runsOf(github, endpoint, args) ?? (endpoint === listPulls ? [pr()] : jobs(pr()));
  github.hook = { before() {} };
  return { github, counters, statuses, stored };
}

async function withPublisher(fn) {
  const directory = await mkdtemp(path.join(os.tmpdir(), 'fork-ci-publisher-test-'));
  const stateFile = path.join(directory, 'state.json');
  const ctx = context('apache/gravitino');ctx.eventName = 'workflow_dispatch';ctx.payload.inputs = { 'pull-request': '7' };
  const failures = [];
  const outputs = {};
  const core = { info() {}, warning() {}, setFailed: message => failures.push(message),
    setOutput: (name, value) => { outputs[name] = value; } };
  try { await fn({ ctx, stateFile, core, failures, outputs }); }
  finally { await rm(directory, { recursive: true, force: true }); }
}
test('publisher links the status to the exact fork run attempt and creates no check run', async () => withPublisher(async ({ ctx, core }) => {
  const { github, statuses } = publisherMock({ rerun: { ...run(), run_attempt: 3 } });
  await update({ github, context: ctx, core, sleep: async () => {} });
  assert.deepEqual(statuses, [{ owner: 'apache', repo: 'gravitino', sha: A, context: 'Fork CI', state: 'success',
    description: 'All required suites passed', target_url: url(1, 3) }]);
  // A check run would be attached to an arbitrary suite and show up twice on the PR.
  assert.equal(github.rest.checks, undefined);
}));

test('a push or retarget during publication cannot publish stale success', async () => withPublisher(async ({ ctx, core }) => {
  const changed = pr(); changed.base.sha = C;
  const { github, statuses } = publisherMock({ current: changed });
  await update({ github, context: ctx, core, sleep: async () => {} });
  assert.equal(statuses.length, 0);
}));

test('manual publication refreshes a final success and observes a failed rerun', async () => withPublisher(async ({ ctx, core }) => {
  const { github, statuses, counters } = publisherMock({ latest: forkStatus('success', 'All required suites passed'),
    rerun: { ...run(), run_attempt: 2, conclusion: 'failure' } });
  await update({ github, context: ctx, core, sleep: async () => {} });
  assert.ok(counters.runs > 0);
  assert.equal(statuses[0].state, 'failure'); assert.equal(statuses[0].target_url, url(1, 2));
}));

test('publisher failures remain observable and are retried instead of cached', async () => withPublisher(async ({ ctx, stateFile, core, failures }) => {
  const { github, statuses } = publisherMock({ failTrees: true });
  await update({ github, context: ctx, core, stateFile, sleep: async () => {} });
  assert.equal(statuses.length, 0); assert.equal(failures.length, 1);
  await assert.rejects(readFile(stateFile, 'utf8'), { code: 'ENOENT' });
}));

test('PR signals prioritize their head and bypass settled state', async () => withPublisher(async ({ ctx, core }) => {
  ctx.eventName = 'workflow_run';delete ctx.payload.inputs;
  ctx.payload.workflow_run = { name: 'Fork CI PR Signal', head_sha: A, event: 'pull_request' };
  const { github, statuses } = publisherMock();
  await update({ github, context: ctx, core, sleep: async () => {} });
  assert.equal(statuses.length, 1);
}));

test('a finished Fork CI push run refreshes only its head once, without polling the signal queue', async () => withPublisher(async ({ ctx, core, outputs }) => {
  ctx.eventName = 'workflow_run';delete ctx.payload.inputs;
  for (const [head, expected] of [[A, 1], [C, 0]]) {
    ctx.payload.workflow_run = { name: 'Fork CI', head_sha: head, event: 'push', display_title: 'Fork CI main' };
    const { github, statuses } = publisherMock();
    github.rest.rateLimit.get = async () => ({ data: { resources: { core: { remaining: 15000, limit: 15000 } } } });
    let sleeps = 0;
    await update({ github, context: ctx, core, sleep: async () => { sleeps++; } });
    assert.equal(sleeps, 0); assert.equal(statuses.length, expected);
    assert.equal(outputs.chain, undefined);
  }
}));

test('scheduled publication ignores poisoned cache state and recomputes definitions', async () => withPublisher(async ({ ctx, stateFile, core }) => {
  ctx.eventName = 'schedule'; delete ctx.payload.inputs;
  await require('node:fs/promises').writeFile(stateFile, JSON.stringify({ cursor: 0, prs: { 7: {
    sha: A, base: B, base_ref: 'main', repository: 'fork/repo', definitions_equal: true,
    updated_at: pr().updated_at, status: 'completed', checked_at: new Date().toISOString(), completed_at: new Date().toISOString()
  } } }));
  const { github, statuses } = publisherMock();
  github.rest.git = serviceMock({ changed: true }).rest.git;
  await update({ github, context: ctx, core, stateFile, sleep: async () => {} });
  assert.equal(statuses[0].description, 'Fork CI passed; trusted verification required');
  const workflow = await readFile(path.join(__dirname, '../../workflows/fork-ci-status-update.yml'), 'utf8');
  assert.ok(!workflow.includes('actions/cache/'));
}));

test('partial reruns retain successful callers but use each job newest result', async () => {
  const p = pr();
  const github = serviceMock();
  github.rest.actions.listWorkflowRuns = async ({ event }) => ({ data: {
    workflow_runs: event !== 'workflow_dispatch' ? [{ ...run(), run_attempt: 2 }] : []
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

test('scheduled sweeps probe recently updated PRs first and refresh only running or rerun results', async () => withPublisher(async ({ ctx, core }) => {
  ctx.eventName = 'schedule'; delete ctx.payload.inputs;
  const { github, statuses, stored } = publisherMock();
  const recent = new Date().toISOString();
  const old = Array.from({ length: 20 }, (_, i) => ({ ...pr(), number: 100 + i,
    head: { ...pr().head, sha: (100 + i).toString(16).padStart(40, 'd') } }));
  const settled = { ...pr(), number: 8, updated_at: recent, head: { ...pr().head, sha: 'e'.repeat(40) } };
  // Any completed result, not only success, is final until its fork run id or attempt changes.
  stored.set(settled.head.sha, [forkStatus('error', 'Fork CI cancelled')]);
  const failed = { ...pr(), updated_at: recent };
  const all = [...old, settled, failed];
  const listPulls = github.rest.pulls.list, paginate = github.paginate;
  github.paginate = async (endpoint, args) => endpoint === listPulls ? all : paginate(endpoint, args);
  github.rest.pulls.get = async ({ pull_number }) => ({ data: all.find(p => p.number === pull_number) });
  const runsFor = [];
  const list = github.rest.actions.listWorkflowRuns;
  github.rest.actions.listWorkflowRuns = async args => {
    runsFor.push(args.head_sha);
    const response = await list(args);
    response.data.workflow_runs = response.data.workflow_runs.map(run => ({ ...run, head_sha: args.head_sha }));
    return response;
  };
  await update({ github, context: ctx, core, sleep: async () => {} });
  // The settled PR is probed once (fork and trusted runs unchanged) and never rewritten.
  assert.equal(runsFor.filter(sha => sha === settled.head.sha).length, 1);
  assert.equal(statuses.filter(s => s.sha === settled.head.sha).length, 0);
  const current = statuses.find(s => s.sha === A);
  assert.equal(current.state, 'success'); assert.equal(current.target_url, url());
}));

test('a failed fork run reports failure and points the author to re-run it in the fork', async () => withPublisher(async ({ ctx, core }) => {
  const { github, statuses } = publisherMock({ rerun: { ...run(), conclusion: 'failure' } });
  await update({ github, context: ctx, core, sleep: async () => {} });
  assert.equal(statuses[0].state, 'failure');
  assert.equal(statuses[0].description, 'Fork CI failure; re-run it in your fork');
  assert.equal(statuses[0].target_url, url());
}));

test('signals queue in order while scheduled sweeps poll in their own group', async () => {
  const workflow = await readFile(path.join(__dirname, '../../workflows/fork-ci-status-update.yml'), 'utf8');
  // No workflow-level group: a polling sweep must never block queued PR signals.
  assert.doesNotMatch(workflow, /^concurrency:/m);
  assert.match(workflow, /^  update:\n(?:.*\n)*?.*github\.event_name != 'schedule'(?:.*\n)*?\s+group: fork-ci-status-update\n\s+queue: max\n\s+cancel-in-progress: false\n/m);
  assert.match(workflow, /^  sweep:\n\s+if: >-\n(?:.*\n)*?.*github\.event_name == 'schedule' \|\| \(github\.event_name == 'workflow_dispatch' && inputs\.sweep\)\)\n(?:.*\n)*?\s+group: fork-ci-status-sweep\n\s+cancel-in-progress: false\n/m);
  // A newer schedule replaces a pending sweep rather than piling up behind 11-minute sweeps.
  assert.doesNotMatch(workflow.slice(workflow.indexOf('\n  sweep:')), /queue:/);
});

test('scheduled sweeps refresh a settled status when its fork run is rerun', async () => withPublisher(async ({ ctx, core }) => {
  ctx.eventName = 'schedule'; delete ctx.payload.inputs;
  const recent = { ...pr(), updated_at: new Date().toISOString() };
  const { github, statuses } = publisherMock({ latest: forkStatus('success', 'All required suites passed'),
    rerun: { ...run(), run_attempt: 2, conclusion: 'failure' } });
  const listPulls = github.rest.pulls.list, paginate = github.paginate;
  github.paginate = async (endpoint, args) => endpoint === listPulls ? [recent] : paginate(endpoint, args);
  github.rest.pulls.get = async () => ({ data: recent });
  await update({ github, context: ctx, core, sleep: async () => {} });
  assert.equal(statuses.length, 1);
  assert.equal(statuses[0].state, 'failure'); assert.equal(statuses[0].target_url, url(1, 2));
}));

test('a publisher that observed an older attempt does not overwrite a newer result', async () => withPublisher(async ({ ctx, core }) => {
  for (const [latest, observed] of [
    [forkStatus('failure', 'Failed: build / build', url(1, 2)), { conclusion: 'success', run_attempt: 1 }],
    [forkStatus('success', 'All required suites passed', url(1, 1)), { status: 'in_progress', conclusion: null, run_attempt: 1 }],
    [forkStatus('success', 'All required suites passed', url(5, 1)), { id: 1, conclusion: 'failure' }]]) {
    const { github, statuses } = publisherMock({ latest, rerun: { ...run(), ...observed } });
    await update({ github, context: ctx, core, sleep: async () => {} });
    assert.equal(statuses.length, 0);
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
    cancelWorkflowRun: async args => { calls.cancel++; calls.run_id = args.run_id; } },
  // No upstream PR for the branch unless a test opens one.
  pulls: { list: async () => ({ data: [] }), get: async () => ({ data: { state: 'open' } }) } },
  paginate: async (endpoint, args) => { calls.attempt = args.attempt_number; return polls[Math.min(calls.polls++, polls.length - 1)]; } };
  const errors = [];
  return { github, calls, errors, context: { repo: { owner: 'fork', repo: 'repo' }, ref: 'refs/heads/topic', runId: 42 },
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

test('every one-minute pass probes all recently updated PRs at the apache quota', async () => withPublisher(async ({ ctx, core }) => {
  ctx.eventName = 'schedule'; delete ctx.payload.inputs;
  const { github } = publisherMock();
  // apache/gravitino has had 13-17 PRs updated within six hours; leave headroom.
  const recent = Array.from({ length: 17 }, (_, i) => ({ ...pr(), number: 300 + i, updated_at: new Date().toISOString(),
    head: { ...pr().head, sha: (300 + i).toString(16).padStart(40, 'e') } }));
  const listPulls = github.rest.pulls.list, paginate = github.paginate;
  let count = () => {};
  github.hook = { before(_, hook) { count = hook; } };
  github.rest.rateLimit.get = async () => ({ data: { resources: { core: { remaining: 15000, limit: 15000 } } } });
  const perPass = [new Set()];
  github.rest.repos.getCombinedStatusForRef = async ({ ref }) => { count(); perPass.at(-1).add(ref);
    return { data: { statuses: [forkStatus('success', 'All required suites passed')] } }; };
  github.paginate = async (endpoint, args) => { count(); return endpoint === listPulls ? recent : paginate(endpoint, args); };
  let t = Date.now();
  await update({ github, context: ctx, core, clock: () => t, sleep: async ms => { t += ms; perPass.push(new Set()); } });
  assert.equal(perPass.length, 11);
  for (const seen of perPass) assert.equal(seen.size, recent.length);
}));

test('passes start on a fixed one-minute cadence, however long a pass takes', async () => withPublisher(async ({ ctx, core }) => {
  ctx.eventName = 'schedule'; delete ctx.payload.inputs;
  const { github } = publisherMock();
  github.rest.rateLimit.get = async () => ({ data: { resources: { core: { remaining: 15000, limit: 15000 } } } });
  let t = 0;
  const starts = [];
  const listPulls = github.rest.pulls.list, paginate = github.paginate;
  // Each pass spends 35 seconds on API calls, as on apache/gravitino.
  github.paginate = async (endpoint, args) => {
    if (endpoint === listPulls) { starts.push(t); t += 35000; }
    return paginate(endpoint, args);
  };
  await update({ github, context: ctx, core, clock: () => t, sleep: async ms => { assert.ok(ms >= 0); t += ms; } });
  assert.deepEqual(starts, Array.from({ length: 11 }, (_, i) => i * 60000));
}));

test('each polling sweep dispatches the next; only that job can write Actions state', async () => {
  const workflow = await readFile(path.join(__dirname, '../../workflows/fork-ci-status-update.yml'), 'utf8');
  assert.match(workflow, /^      sweep:\n\s+description: .*\n\s+required: false\n\s+type: boolean\n/m);
  assert.match(workflow, /!\(github\.event_name == 'workflow_dispatch' && inputs\.sweep\)/);
  assert.match(workflow, /outputs:\n\s+chain: \$\{\{ steps\.sweep\.outputs\.chain \}\}\n/);
  const rearm = workflow.slice(workflow.indexOf('\n  rearm:'));
  assert.match(rearm, /needs: sweep\n\s+if: \$\{\{ !cancelled\(\) && needs\.sweep\.outputs\.chain == 'true' \}\}/);
  assert.match(rearm, /permissions:\n\s+actions: write\n\s+steps:/);
  assert.doesNotMatch(rearm, /checkout|require\(/);
  assert.match(rearm, /context\.ref !== `refs\/heads\/\$\{repository\.default_branch\}`/);
  assert.match(rearm, /createWorkflowDispatch\(\{[^}]*workflow_id: 'fork-ci-status-update\.yml', ref: repository\.default_branch, inputs: \{ sweep: 'true' \}/);
  // The publishers themselves never get write access to Actions.
  assert.equal(workflow.match(/^\s+actions: write$/gm).length, 1);
});

test('a dispatched sweep polls like a scheduled one and continues the chain only when polling', async () => {
  const run = async (eventName, inputs, limit) => {
    let observed;
    await withPublisher(async ({ ctx, core, outputs }) => {
      ctx.eventName = eventName; ctx.payload.inputs = inputs;
      const { github } = publisherMock();
      github.rest.rateLimit.get = async () => ({ data: { resources: { core: { remaining: limit, limit } } } });
      let sleeps = 0;
      await update({ github, context: ctx, core, sleep: async () => { sleeps++; } });
      observed = { chain: outputs.chain, passes: sleeps + 1 };
    });
    return observed;
  };
  assert.deepEqual(await run('workflow_dispatch', { sweep: 'true', 'pull-request': '' }, 15000), { chain: 'true', passes: 11 });
  assert.deepEqual(await run('workflow_dispatch', { sweep: true }, 15000), { chain: 'true', passes: 11 });
  assert.deepEqual(await run('schedule', undefined, 15000), { chain: 'true', passes: 11 });
  // A single-pass sweep at a small quota must not run back to back.
  assert.deepEqual(await run('schedule', undefined, 1000), { chain: 'false', passes: 1 });
  // A manual refresh is one pass and never starts a chain.
  assert.deepEqual(await run('workflow_dispatch', { 'pull-request': '7' }, 15000), { chain: undefined, passes: 1 });
});

test('Run workflow in a fork tests the selected branch like a push', async () => {
  const output = {};
  const ctx = context('fork/repo', true);ctx.eventName = 'workflow_dispatch';ctx.payload.inputs = { 'pull-request': '' };
  await resolve({ context: ctx, core: coreFor(output), github: {
    rest: { pulls: { list() {} }, repos: { get: async () => ({ data: { fork: true, source: { full_name: 'apache/gravitino' } } }),
      getBranch: async () => ({ data: { commit: { sha: B } } }) } }, paginate: async () => [pr()] } });
  assert.equal(output.sync, 'true'); assert.equal(output['base-sha'], B); assert.equal(output['head-sha'], A);
  assert.equal(output['head-repository'], 'fork/repo');
  // Naming a PR still means a trusted run, which only apache/gravitino may dispatch.
  ctx.payload.inputs = { 'pull-request': '7' };
  await assert.rejects(resolve({ context: ctx, core: coreFor({}), github: {} }), /apache/);
  const upstream = context('apache/gravitino');upstream.eventName = 'workflow_dispatch';upstream.payload.inputs = { 'pull-request': '' };
  await assert.rejects(resolve({ context: upstream, core: coreFor({}), github: {} }), /pull request number is required/);
});

test('a push and Run workflow in the fork share one cancel-in-progress group per branch', async () => {
  const workflow = await readFile(path.join(__dirname, '../../workflows/fork-ci.yml'), 'utf8');
  assert.match(workflow, /pull-request:\n\s+description: .*\n\s+required: false\n/);
  assert.match(workflow, /group: fork-ci-\$\{\{ inputs\.pull-request && format\('trusted-\{0\}', inputs\.pull-request\) \|\| github\.ref \}\}\n\s+cancel-in-progress: true/);
  assert.match(workflow, /github\.event_name == 'workflow_dispatch' && inputs\.pull-request != '' && format\('Trusted Fork CI #\{0\}'/);
});

test('the publisher mirrors fork runs started with Run workflow, and only Fork CI events', async () => {
  const github = serviceMock();
  github.rest.actions.listWorkflowRuns = async ({ event }) => ({ data: { workflow_runs: event === 'workflow_dispatch' ? []
    : [{ ...run(), id: 5, event: 'workflow_dispatch', conclusion: 'failure' }, { ...run(), id: 9, event: 'schedule' }, run()] } });
  github.paginate = async (endpoint, args) => runsOf(github, endpoint, args) ??
    (args.run_id === 5 ? [...jobs(pr()), { name: 'build / build', conclusion: 'failure' }] : jobs(pr()));
  const mirrored = await createService(github).mirror(pr());
  assert.equal(mirrored.run.id, 5); assert.equal(mirrored.result.title, 'Failed: build / build');
});

test('a fork run stops itself once its pull request is closed', async () => {
  const running = [job('build / build', 'in_progress')], done = [job('build / build', 'completed', 'success')];
  // Without the closed-PR check the watcher would wait for the jobs to finish and never cancel.
  const mock = watcherMock([running, running, running, running, done, done]);
  let state = 'open', lookups = 0;
  mock.github.rest.pulls.list = async args => {
    assert.deepEqual([args.owner, args.repo, args.head, args.state], ['apache', 'gravitino', 'fork:topic', 'open']);
    // The PR is opened after the run started.
    return { data: lookups++ ? [{ number: 7, head: { repo: { full_name: 'fork/repo' } } }] : [] };
  };
  mock.github.rest.pulls.get = async args => { assert.equal(args.pull_number, 7); const now = state; state = 'closed'; return { data: { state: now } }; };
  const notices = [];
  mock.core.notice = message => notices.push(message);
  assert.deepEqual(await watch(mock), []);
  assert.equal(mock.calls.cancel, 1); assert.equal(mock.calls.polls, 4);
  assert.match(notices[0], /apache\/gravitino#7 was closed.*re-run all jobs or start a new run with Run workflow/);
});

test('closed-PR checks never cancel upstream runs and tolerate API errors', async () => {
  const running = [job('build / build', 'in_progress')], done = [job('build / build', 'completed', 'success')];
  const upstream = watcherMock([running, done, done]);
  upstream.context.repo = { owner: 'apache', repo: 'gravitino' };
  upstream.github.rest.pulls.list = async () => { throw new Error('must not look up PRs upstream'); };
  assert.deepEqual(await watch(upstream), []);
  assert.equal(upstream.calls.cancel, 0);
  const flaky = watcherMock([running, done, done]);
  const warnings = [];
  flaky.core.warning = message => warnings.push(message);
  flaky.github.rest.pulls.list = async () => { throw new Error('Server Error'); };
  assert.deepEqual(await watch(flaky), []);
  assert.equal(flaky.calls.cancel, 0); assert.match(warnings[0], /upstream pull request: Server Error/);
});

test('failed and cancelled fork results suggest re-running in the fork when it fits', async () => withPublisher(async ({ ctx, core }) => {
  for (const [rerun, forkJobs, expected] of [
    [{ ...run(), conclusion: 'cancelled' }, jobs(pr()), 'Fork CI cancelled; re-run it in your fork'],
    [{ ...run(), conclusion: 'failure' }, [...jobs(pr()), { name: 'x'.repeat(130), conclusion: 'failure' }], `Failed: ${'x'.repeat(130)}`.slice(0, 140)],
    [run(), jobs(pr()), 'All required suites passed']]) {
    const { github, statuses } = publisherMock({ rerun });
    github.paginate = (paginate => async (endpoint, args) => args?.run_id ? forkJobs : paginate(endpoint, args))(github.paginate);
    await update({ github, context: ctx, core, sleep: async () => {} });
    assert.equal(statuses[0].description, expected);
  }
}));

test('transient API errors never fail the watcher or the run', async () => {
  const mock = watcherMock([[job('build / build', 'in_progress')],
    [job('build / build', 'completed', 'success')], [job('build / build', 'completed', 'success')]]);
  const paginate = mock.github.paginate;
  let errors = 2;
  mock.github.paginate = async (...args) => {
    if (errors-- > 0) throw Object.assign(new Error('Server Error'), { status: 500 });
    return paginate(...args);
  };
  const warnings = [];
  mock.core.warning = message => warnings.push(message);
  assert.deepEqual(await watch(mock), []);
  assert.equal(warnings.length, 2); assert.match(warnings[0], /Server Error\. Retrying in a minute/);
  assert.equal(mock.errors.at(-1), 'All jobs completed without a failure.');
});

test('a failed cancel request is retried on the next poll', async () => {
  const failing = [job('backend / BackendIT (h2)', 'completed', 'failure')];
  const mock = watcherMock([failing, failing]);
  const cancel = mock.github.rest.actions.cancelWorkflowRun;
  let first = true;
  mock.github.rest.actions.cancelWorkflowRun = async args => {
    if (first) { first = false; throw new Error('Server Error'); }
    return cancel(args);
  };
  mock.core.warning = () => {};
  assert.deepEqual(await watch(mock), ['backend / BackendIT (h2)']);
  assert.equal(mock.calls.cancel, 1); assert.equal(mock.calls.polls, 2);
});

test('the watcher cannot fail the run, and a watcher-only failure is judged by the suites', async () => {
  const workflow = await readFile(path.join(__dirname, '../../workflows/fork-ci.yml'), 'utf8');
  assert.match(workflow, /fail_fast:\n(?:.*\n)*?\s+continue-on-error: true\n/);
  assert.match(workflow, new RegExp(`self: '${FAIL_FAST}'`));
  const p = pr();
  // Runs started before continue-on-error: only the watcher failed, every suite passed.
  const watcherFailed = [...jobs(p), { name: FAIL_FAST, conclusion: 'failure' }];
  const result = evaluate(p, { ...run(), conclusion: 'failure' }, watcherFailed);
  assert.equal(result.conclusion, 'success'); assert.equal(result.title, 'All required suites passed');
  // A real failure is still reported, never the watcher.
  const both = [...watcherFailed, { name: 'backend / BackendIT (h2)', conclusion: 'failure' }];
  assert.equal(evaluate(p, { ...run(), conclusion: 'failure' }, both).title, 'Failed: backend / BackendIT (h2)');
  // Without a passing Required CI job the run still cannot be green.
  const noRequired = watcherFailed.filter(j => j.name !== requiredName(p));
  assert.equal(evaluate(p, { ...run(), conclusion: 'failure' }, noRequired).conclusion, 'action_required');
});

const notFound = () => Object.assign(new Error('Not Found'), { status: 404 });

test('a fork without the Fork CI workflow asks the author to update the branch', async () => {
  const github = serviceMock();
  github.rest.actions.listWorkflowRuns = async ({ event }) => {
    if (event !== 'workflow_dispatch') throw notFound();
    return { data: { workflow_runs: [] } };
  };
  const { result } = await createService(github).mirror(pr());
  assert.equal(result.status, 'completed'); assert.equal(result.conclusion, 'action_required');
  assert.match(result.title, /never run in your fork; merge the latest base branch/);
  assert.match(result.note, /adds \.github\/workflows\/fork-ci\.yml/);
  // Other errors are still failures of the publisher, not of the PR.
  github.rest.actions.listWorkflowRuns = async () => { throw Object.assign(new Error('Server Error'), { status: 500 }); };
  await assert.rejects(createService(github).mirror(pr()), /Server Error/);
});

test('sweeps publish the missing-workflow status once and then leave it settled', async () => withPublisher(async ({ ctx, core, failures }) => {
  ctx.eventName = 'schedule'; delete ctx.payload.inputs;
  const recent = { ...pr(), head: { ...pr().head, ref: 'topic' }, updated_at: new Date().toISOString() };
  const { github, statuses } = publisherMock();
  github.rest.actions.listWorkflowRuns = async ({ event }) => {
    if (event !== 'workflow_dispatch') throw notFound();
    return { data: { workflow_runs: [] } };
  };
  const listPulls = github.rest.pulls.list, paginate = github.paginate;
  github.paginate = async (endpoint, args) => endpoint === listPulls ? [recent] : paginate(endpoint, args);
  github.rest.pulls.get = async () => ({ data: recent });
  github.rest.rateLimit.get = async () => ({ data: { resources: { core: { remaining: 15000, limit: 15000 } } } });
  await update({ github, context: ctx, core, sleep: async () => {} });
  assert.deepEqual(failures, []);
  assert.equal(statuses.length, 1); assert.equal(statuses[0].state, 'error');
  assert.match(statuses[0].description, /never run in your fork/);
  // There is no run to open; link to the fork's Fork CI page, where Actions can be enabled.
  assert.equal(statuses[0].target_url, 'https://github.com/fork/repo/actions/workflows/fork-ci.yml');
}));

test('scheduled sweeps scale their budget with the hourly quota', async () => withPublisher(async ({ ctx, stateFile, core }) => {
  ctx.eventName = 'schedule'; delete ctx.payload.inputs;
  const probed = async limit => {
    const { github } = publisherMock();
    const recent = Array.from({ length: 60 }, (_, i) => ({ ...pr(), number: 200 + i, updated_at: new Date().toISOString(),
      head: { ...pr().head, sha: (200 + i).toString(16).padStart(40, 'f') } }));
    const listPulls = github.rest.pulls.list, paginate = github.paginate;
    // Count requests, which is what each probe makes.
    let count = () => {};
    github.hook = { before(_, hook) { count = hook; } };
    github.rest.rateLimit.get = async () => ({ data: { resources: { core: { remaining: limit, limit } } } });
    const seen = new Set();
    github.rest.repos.getCombinedStatusForRef = async ({ ref }) => { count(); seen.add(ref);
      return { data: { statuses: [forkStatus('success', 'All required suites passed')] } }; };
    github.paginate = async (endpoint, args) => { count(); return endpoint === listPulls ? recent : paginate(endpoint, args); };
    // Each pass spends its share of the sweep budget, then the sweep sleeps until the next pass.
    let t = Date.now(), passes = 0;
    await update({ github, context: ctx, core, stateFile, clock: () => t, sleep: async ms => { t += ms; passes++; } });
    return { seen: seen.size, passes: passes + 1 };
  };
  // Small quotas keep one pass; large ones poll every minute within the 11-minute sweep.
  const small = await probed(1000), large = await probed(15000);
  assert.equal(small.passes, 1); assert.ok(small.seen < 60);
  assert.equal(large.passes, 11); assert.equal(large.seen, 60);
}));

test('a missing fork run waits instead of asking the author to act', async () => {
  const github = serviceMock();
  github.rest.actions.listWorkflowRuns = async () => ({ data: { workflow_runs: [] } });
  const { result } = await createService(github).mirror(pr());
  assert.equal(result.status, 'queued'); assert.equal(result.conclusion, undefined);
  assert.match(result.note, /enable Actions in your fork/);
});

test('an incomplete run listing does not replace a status that links a run', async () => withPublisher(async ({ ctx, core }) => {
  const { github, statuses } = publisherMock({ latest: forkStatus('success', 'All required suites passed') });
  github.rest.actions.listWorkflowRuns = async () => ({ data: { workflow_runs: [] } });
  await update({ github, context: ctx, core, sleep: async () => {} });
  assert.equal(statuses.length, 0);
}));

test('successive scheduled sweeps mirror a fork run from running to success', async () => withPublisher(async ({ ctx, core }) => {
  ctx.eventName = 'schedule'; delete ctx.payload.inputs;
  const recent = { ...pr(), updated_at: new Date().toISOString() };
  const { github, statuses } = publisherMock({ rerun: { ...run(), status: 'in_progress', conclusion: null } });
  const listPulls = github.rest.pulls.list, paginate = github.paginate, list = github.rest.actions.listWorkflowRuns;
  github.paginate = async (endpoint, args) => endpoint === listPulls ? [recent] : paginate(endpoint, args);
  github.rest.pulls.get = async () => ({ data: recent });
  github.rest.rateLimit.get = async () => ({ data: { resources: { core: { remaining: 15000, limit: 15000 } } } });
  let t = Date.now(), finished = false;
  github.rest.actions.listWorkflowRuns = async args => {
    const response = await list(args);
    if (finished) response.data.workflow_runs = response.data.workflow_runs.map(r => ({ ...r, status: 'completed', conclusion: 'success' }));
    return response;
  };
  await update({ github, context: ctx, core, clock: () => t, sleep: async ms => { t += ms; } });
  finished = true; t += 300000;
  await update({ github, context: ctx, core, clock: () => t, sleep: async ms => { t += ms; } });
  assert.deepEqual(statuses.map(s => s.state), ['pending', 'success']);
}));

test('queued workflow summaries use the actual job state for the PR description', () => {
  const queued = { ...run(), status: 'queued', conclusion: null };
  const waiting = evaluate(pr(), queued, [{ name: 'build / build', status: 'queued' }]);
  assert.equal(waiting.status, 'queued');
  assert.match(waiting.title, /waiting for a runner/);
  const running = evaluate(pr(), queued, [{ name: 'build / build', status: 'in_progress' }]);
  assert.equal(running.status, 'in_progress');
  assert.equal(running.title, 'Fork CI is running');
});

test('fork results publish a visible commit status for every lifecycle state', async () => withPublisher(async ({ ctx, core }) => {
  for (const [status, conclusion, expected, description] of [
    ['queued', null, 'pending', 'Fork CI is queued, waiting for a runner'], ['in_progress', null, 'pending', 'Fork CI is running'],
    ['completed', 'success', 'success', 'All required suites passed'],
    ['completed', 'failure', 'failure', 'Fork CI failure; re-run it in your fork'],
    ['completed', 'timed_out', 'failure', 'Fork CI timed_out; re-run it in your fork'],
    ['completed', 'cancelled', 'error', 'Fork CI cancelled; re-run it in your fork']]) {
    const { github, statuses } = publisherMock({ rerun: { ...run(), status, conclusion } });
    await update({ github, context: ctx, core, sleep: async () => {} });
    assert.deepEqual(statuses, [{ owner: 'apache', repo: 'gravitino', sha: A,
      context: 'Fork CI', state: expected, description, target_url: url() }]);
  }
}));

test('missing fork runs stay pending and link to the fork Fork CI page instead of a nonexistent run', async () => withPublisher(async ({ ctx, core }) => {
  const { github, statuses } = publisherMock();
  github.rest.actions.listWorkflowRuns = async () => ({ data: { workflow_runs: [] } });
  await update({ github, context: ctx, core, sleep: async () => {} });
  assert.equal(statuses[0].state, 'pending');
  assert.equal(statuses[0].description, 'Waiting for Fork CI; enable Actions in your fork if no run starts');
  assert.equal(statuses[0].target_url, 'https://github.com/fork/repo/actions/workflows/fork-ci.yml');
}));

test('changed CI definitions stay pending, not red, until trusted CI verifies them', async () => withPublisher(async ({ ctx, core }) => {
  const { github, statuses } = publisherMock();
  github.rest.git = serviceMock({ changed: true }).rest.git;
  await update({ github, context: ctx, core, sleep: async () => {} });
  // Waiting for a committer is not a failure of the contributor's change.
  assert.equal(statuses[0].state, 'pending');
  assert.equal(statuses[0].description, 'Fork CI passed; trusted verification required');
}));

test('CI edits mirror execution progress and failures without self-approving success', async () => withPublisher(async ({ ctx, core }) => {
  for (const [status, conclusion, state, description] of [
    ['queued', null, 'pending', 'Fork CI is queued, waiting for a runner'],
    ['in_progress', null, 'pending', 'Fork CI is running'],
    ['completed', 'failure', 'failure', 'Fork CI failure; re-run it in your fork'],
    ['completed', 'cancelled', 'error', 'Fork CI cancelled; re-run it in your fork'],
    ['completed', 'success', 'pending', 'Fork CI passed; trusted verification required']]) {
    const { github, statuses } = publisherMock({ rerun: { ...run(), status, conclusion } });
    github.rest.git = serviceMock({ changed: true }).rest.git;
    await update({ github, context: ctx, core, sleep: async () => {} });
    assert.deepEqual([statuses[0].state, statuses[0].description, statuses[0].target_url], [state, description, url()]);
  }
}));

test('scheduled publishers repair legacy statuses without an attempt while the run is still running', async () => withPublisher(async ({ ctx, core }) => {
  ctx.eventName = 'schedule'; delete ctx.payload.inputs;
  const recent = { ...pr(), updated_at: new Date().toISOString() };
  // Older publishers linked the run without its attempt and completed CI edits prematurely.
  const legacy = forkStatus('error', 'Fork CI needs attention', 'https://github.com/fork/repo/actions/runs/1');
  const { github, statuses } = publisherMock({ latest: legacy, rerun: { ...run(), status: 'in_progress', conclusion: null } });
  github.rest.git = serviceMock({ changed: true }).rest.git;
  const listPulls = github.rest.pulls.list, paginate = github.paginate;
  github.paginate = async (endpoint, args) => endpoint === listPulls ? [recent] : paginate(endpoint, args);
  github.rest.pulls.get = async () => ({ data: recent });
  await update({ github, context: ctx, core, sleep: async () => {} });
  assert.equal(statuses.length, 1);
  assert.equal(statuses[0].state, 'pending'); assert.equal(statuses[0].target_url, url());
}));

test('a completed execution cannot regress to running on a stale API response', async () => withPublisher(async ({ ctx, core }) => {
  for (const latest of [forkStatus('pending', 'Fork CI passed; trusted verification required'),
    forkStatus('failure', 'Failed: build / build')]) {
    const { github, statuses } = publisherMock({ latest, rerun: { ...run(), status: 'in_progress', conclusion: null } });
    github.rest.git = serviceMock({ changed: true }).rest.git;
    await update({ github, context: ctx, core, sleep: async () => {} });
    assert.equal(statuses.length, 0);
  }
}));

test('scheduled probes repair statuses that link another run or attempt', async () => withPublisher(async ({ ctx, core }) => {
  ctx.eventName = 'schedule'; delete ctx.payload.inputs;
  const recent = { ...pr(), updated_at: new Date().toISOString() };
  for (const wrong of [url(0, 1), 'https://github.com/fork/repo/actions/runs/1']) {
    const { github, statuses } = publisherMock({ latest: forkStatus('success', 'All required suites passed', wrong) });
    const listPulls = github.rest.pulls.list, paginate = github.paginate;
    github.paginate = async (endpoint, args) => endpoint === listPulls ? [recent] : paginate(endpoint, args);
    github.rest.pulls.get = async () => ({ data: recent });
    await update({ github, context: ctx, core, sleep: async () => {} });
    assert.equal(statuses.length, 1);
    assert.equal(statuses[0].target_url, url());
  }
}));

test('scheduled probes clear settled success when trusted CI is dispatched or rerun', async () => withPublisher(async ({ ctx, core }) => {
  ctx.eventName = 'schedule'; delete ctx.payload.inputs;
  const recent = { ...pr(), updated_at: new Date().toISOString() };
  for (const wasTrusted of [false, true]) {
    const shown = wasTrusted ? `${url(2, 1, 'apache', 'gravitino')}#fork-run-1-1` : url();
    const { github, statuses } = publisherMock({ latest: forkStatus('success', 'All required suites passed', shown) });
    github.rest.actions.listWorkflowRuns = async ({ event }) => ({ data: { workflow_runs: event !== 'workflow_dispatch' ? [run()]
      : [{ ...run(), id: 2, head_sha: B, head_branch: 'main', run_attempt: wasTrusted ? 2 : 1,
        status: 'in_progress', conclusion: null, display_title: 'Trusted Fork CI #7', created_at: '2026-01-02T00:00:00Z' }] } });
    const listPulls = github.rest.pulls.list, paginate = github.paginate;
    github.paginate = async (endpoint, args) => endpoint === listPulls ? [recent] : paginate(endpoint, args);
    github.rest.pulls.get = async () => ({ data: recent });
    await update({ github, context: ctx, core, sleep: async () => {} });
    assert.equal(statuses[0].state, 'pending');
    // A trusted result also records the fork run it was chosen over.
    assert.equal(statuses[0].target_url, `${url(2, wasTrusted ? 2 : 1, 'apache', 'gravitino')}#fork-run-1-1`);
  }
}));

test('a trusted result stays settled until its fork run changes', async () => withPublisher(async ({ ctx, core }) => {
  ctx.eventName = 'schedule'; delete ctx.payload.inputs;
  const recent = { ...pr(), updated_at: new Date().toISOString() };
  for (const [forkAttempt, writes] of [[1, 0], [2, 1]]) {
    const shown = `${url(2, 1, 'apache', 'gravitino')}#fork-run-1-1`;
    const { github, statuses } = publisherMock({ latest: forkStatus('success', 'All required suites passed', shown) });
    github.rest.actions.listWorkflowRuns = async ({ event }) => ({ data: { workflow_runs: event !== 'workflow_dispatch'
      ? [{ ...run(), run_attempt: forkAttempt }]
      : [{ ...run(), id: 2, head_sha: B, head_branch: 'main', display_title: 'Trusted Fork CI #7', created_at: '2026-01-02T00:00:00Z' }] } });
    const listPulls = github.rest.pulls.list, paginate = github.paginate;
    github.paginate = async (endpoint, args) => endpoint === listPulls ? [recent] : paginate(endpoint, args);
    github.rest.pulls.get = async () => ({ data: recent });
    let refreshed = false;
    const getTree = github.rest.git.getTree;
    github.rest.git.getTree = async args => { refreshed = true; return getTree(args); };
    await update({ github, context: ctx, core, sleep: async () => {} });
    assert.equal(refreshed, forkAttempt === 2);
    assert.equal(statuses.length, writes);
  }
}));

test('CI edits keep the failed job visible after fail-fast cancellation', async () => {
  const github = serviceMock({ changed: true, forkJobs: [{ name: 'build / build', conclusion: 'failure' }] });
  github.rest.actions.listWorkflowRuns = async ({ event }) => ({ data: { workflow_runs:
    event !== 'workflow_dispatch' ? [{ ...run(), conclusion: 'cancelled' }] : [] } });
  const { result } = await createService(github).mirror(pr());
  assert.equal(result.conclusion, 'failure');
  assert.equal(result.title, 'Failed: build / build');
  assert.match(result.note, /committer must dispatch/);
});

test('trusted verification of CI edits mirrors running, failure and success', async () => {
  for (const [status, conclusion] of [['in_progress', null], ['completed', 'failure'], ['completed', 'success']]) {
    const github = serviceMock({ changed: true, trusted: true });
    const list = github.rest.actions.listWorkflowRuns;
    github.rest.actions.listWorkflowRuns = async args => {
      const response = await list(args);
      if (args.event === 'workflow_dispatch') Object.assign(response.data.workflow_runs[0], { status, conclusion });
      return response;
    };
    const mirrored = await createService(github).mirror(pr());
    assert.equal(mirrored.trusted, true);
    assert.equal(mirrored.result.status, status);
    assert.equal(mirrored.result.conclusion || null, conclusion);
  }
});

test('an identical result is not written to the status history again', async () => withPublisher(async ({ ctx, core }) => {
  const { github, statuses } = publisherMock();
  await update({ github, context: ctx, core, sleep: async () => {} });
  await update({ github, context: ctx, core, sleep: async () => {} });
  assert.equal(statuses.length, 1);
}));

test('a push, retarget or newer attempt suppresses stale statuses', async () => withPublisher(async ({ ctx, core }) => {
  for (const change of [p => p.head.sha = C, p => p.base.ref = 'branch-1.3', p => p.base.sha = C, p => p.state = 'closed']) {
    const changed = pr(); change(changed);
    const { github, statuses } = publisherMock({ current: changed });
    await update({ github, context: ctx, core, sleep: async () => {} });
    assert.equal(statuses.length, 0);
  }
  const { github, statuses } = publisherMock({ latest: forkStatus('failure', 'Failed: build / build', url(1, 2)) });
  await update({ github, context: ctx, core, sleep: async () => {} });
  assert.equal(statuses.length, 0);
}));

test('a failed status write is reported and retried by the next publisher', async () => withPublisher(async ({ ctx, core, failures }) => {
  const { github, statuses } = publisherMock();
  const create = github.rest.repos.createCommitStatus;
  github.rest.repos.createCommitStatus = async () => { throw new Error('status API unavailable'); };
  await update({ github, context: ctx, core, sleep: async () => {} });
  assert.match(failures[0], /#7/);
  assert.equal(statuses.length, 0);
  github.rest.repos.createCommitStatus = create;
  await update({ github, context: ctx, core, sleep: async () => {} });
  assert.equal(statuses.length, 1);
}));

const { publishStatus } = require('../publish-ci-status.cjs');
test('unchanged commit statuses do not flood the status history; new states and URLs do publish', async () => {
  const { github, statuses } = publisherMock();
  const params = { github, repo: { owner: 'apache', repo: 'gravitino' }, sha: A,
    context: 'Fork CI', state: 'pending', description: 'Fork CI is running',
    targetUrl: 'https://github.com/fork/repo/actions/runs/1' };
  await publishStatus(params);
  await publishStatus(params);
  assert.equal(statuses.length, 1);
  await publishStatus({ ...params, state: 'success' });
  await publishStatus({ ...params, targetUrl: params.targetUrl + '/attempts/2' });
  assert.equal(statuses.length, 3);
  await publishStatus({ ...params, description: 'x'.repeat(200) });
  assert.equal(statuses.at(-1).description.length, 140);
});

test('allowlist scans publish their actual verdict as a commit status and ignore stale heads', async () => {
  const { publish } = require('../allowlist-pr.cjs');
  const saved = { SCANNED_PRS: process.env.SCANNED_PRS, SCAN_OUTCOME: process.env.SCAN_OUTCOME };
  const ctx = { ...context('apache/gravitino'), serverUrl: 'https://github.com', runId: 123 };
  try {
    process.env.SCANNED_PRS = JSON.stringify([{ number: 7, sha: A }]);
    for (const outcome of ['success', 'failure', 'cancelled', 'skipped']) {
      process.env.SCAN_OUTCOME = outcome;
      const { github, statuses } = publisherMock();
      const failures = [];
      await publish({ github, context: ctx, core: { setFailed: error => failures.push(error) } });
      assert.equal(statuses[0].context, 'ASF Allowlist Check');
      assert.equal(statuses[0].sha, A);
      assert.equal(statuses[0].target_url, 'https://github.com/apache/gravitino/actions/runs/123');
      assert.equal(statuses[0].state, outcome === 'success' ? 'success' : 'failure');
      assert.equal(failures.length, outcome === 'success' ? 0 : 1);
    }
    const { github, statuses } = publisherMock();
    github.rest.pulls.get = async () => ({ data: { ...pr(), head: { ...pr().head, sha: C } } });
    await publish({ github, context: ctx, core: { setFailed() {} } });
    assert.equal(statuses.length, 0);
  } finally {
    for (const [key, value] of Object.entries(saved)) {
      if (value === undefined) delete process.env[key]; else process.env[key] = value;
    }
  }
});

test('only trusted publishers receive the status-writing permission, and nobody creates check runs', async () => {
  for (const workflow of ['fork-ci-status-update.yml', 'asf-allowlist-trusted.yml']) {
    const yaml = await readFile(path.join(__dirname, '../../workflows', workflow), 'utf8');
    assert.match(yaml, /^permissions: \{\}/m);
    assert.match(yaml, /^      statuses: write$/m);
    assert.doesNotMatch(yaml, /checks: write/);
  }
  const signal = await readFile(path.join(__dirname, '../../workflows/fork-ci-status.yml'), 'utf8');
  assert.doesNotMatch(signal, /statuses: write/);
  for (const script of ['update-fork-ci.cjs', 'retire-fork-ci.cjs', 'allowlist-pr.cjs', 'fork-ci-status.cjs', 'publish-ci-status.cjs']) {
    assert.doesNotMatch(await readFile(path.join(__dirname, '..', script), 'utf8'), /\.checks\./, script);
  }
});

test('scheduled probes publish a status that was never written', async () => withPublisher(async ({ ctx, core }) => {
  ctx.eventName = 'schedule'; delete ctx.payload.inputs;
  const recent = { ...pr(), updated_at: new Date().toISOString() };
  const { github, statuses } = publisherMock();
  const listPulls = github.rest.pulls.list, paginate = github.paginate;
  github.paginate = async (endpoint, args) => endpoint === listPulls ? [recent] : paginate(endpoint, args);
  github.rest.pulls.get = async () => ({ data: recent });
  await update({ github, context: ctx, core, sleep: async () => {} });
  assert.equal(statuses.length, 1);
  assert.equal(statuses[0].state, 'success');
}));

test('allowlist verdicts share one serialized queue', async () => {
  const yaml = await readFile(path.join(__dirname, '../../workflows/asf-allowlist-trusted.yml'), 'utf8');
  assert.match(yaml, /^  group: asf-allowlist-publish$/m);
  assert.match(yaml, /^  queue: max$/m);
  assert.match(yaml, /^  cancel-in-progress: false$/m);
});

test('the publisher reads the status last, so a result a concurrent writer just stored wins', async () => withPublisher(async ({ ctx, core }) => {
  // This publisher still sees the run in progress; a concurrent signal stores success meanwhile.
  const { github, statuses, stored } = publisherMock({ rerun: { ...run(), status: 'in_progress', conclusion: null } });
  const get = github.rest.pulls.get;
  let reads = 0;
  github.rest.pulls.get = async args => {
    if (++reads === 2) stored.set(A, [forkStatus('success', 'All required suites passed')]);
    return get(args);
  };
  await update({ github, context: ctx, core, sleep: async () => {} });
  assert.equal(statuses.length, 0);
  assert.equal(stored.get(A)[0].state, 'success');
}));

test('the publisher settles cancelled previous-head statuses after publishing the current head', async () => withPublisher(async ({ ctx, core, failures }) => {
  const p = pr();p.head.ref = 'topic';
  const { github, statuses, stored } = publisherMock();
  github.rest.pulls.get = async () => ({ data: p });
  const oldRun = { ...run(), id: 10, head_sha: C, head_branch: 'topic', event: 'push', conclusion: 'cancelled' };
  stored.set(C, [forkStatus('pending', 'Fork CI is running', url(10, 1))]);
  const list = github.rest.actions.listWorkflowRuns;
  github.rest.actions.listWorkflowRuns = async args => args.branch === 'topic'
    ? { data: { workflow_runs: [oldRun] } } : list(args);
  github.rest.actions.getWorkflowRunAttempt = async () => ({ data: oldRun });
  await update({ github, context: ctx, core, sleep: async () => {} });
  assert.equal(failures.length, 0);
  assert.deepEqual(statuses.map(s => [s.sha, s.state, s.target_url]), [[A, 'success', url()], [C, 'error', url(10, 1)]]);
  assert.equal(statuses[1].description, 'Fork CI cancelled for a previous commit');
}));

test('history cleanup failure leaves the current result published and remains observable', async () => withPublisher(async ({ ctx, core, failures }) => {
  const p = pr();p.head.ref = 'topic';
  const { github, statuses } = publisherMock();github.rest.pulls.get = async () => ({ data: p });
  const list = github.rest.actions.listWorkflowRuns;
  github.rest.actions.listWorkflowRuns = async args => {
    if (args.branch === 'topic') throw new Error('History unavailable');
    return list(args);
  };
  await update({ github, context: ctx, core, sleep: async () => {} });
  assert.equal(statuses[0].sha, A);assert.equal(statuses[0].state, 'success');
  assert.match(failures[0], /#7/);
}));
