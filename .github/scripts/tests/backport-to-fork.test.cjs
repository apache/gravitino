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
const { mkdtempSync, mkdirSync, writeFileSync, readFileSync, rmSync } = require('node:fs');
const { tmpdir } = require('node:os');
const path = require('node:path');
const { execFileSync } = require('node:child_process');
const { resolve, mergedChangeBase, verifyBot, buildCommit, prepare, openPullRequest, dispatch, ROUTING_MARKER } = require('../backport-to-fork.cjs');
const { resolve: resolveCI } = require('../resolve-fork-ci.cjs');
const A = 'a'.repeat(40), B = 'b'.repeat(40);
const task = { owner: 'alice', repo: 'gravitino', repositoryId: 42, number: 7, sha: A, sourceBaseSha: B, title: 'Fix $(untrusted) `title`' };
const source = () => ({ number: 7, merged: true, base: { ref: 'main', repo: { full_name: 'apache/gravitino' } },
  merge_commit_sha: A, labels: [{ name: 'branch-2.0' }], merged_by: { login: 'alice', type: 'User' }, title: task.title });
const fork = () => ({ id: 42, owner: { login: 'alice' }, fork: true, source: { full_name: 'apache/gravitino' } });
function harness() {
  const outputs = {}, secrets = [], calls = [];
  const core = { setOutput: (key, value) => outputs[key] = value, setSecret: value => secrets.push(value), notice() {}, warning() {},
    summary: { addLink() { return this; }, async write() {} } };
  const context = { repo: { owner: 'apache', repo: 'gravitino' }, ref: 'refs/heads/main',
    eventName: 'workflow_dispatch', payload: { repository: { default_branch: 'main' }, inputs: { 'pull-request': '7' } } };
  const github = { rest: { pulls: { get: async () => ({ data: source() }) }, repos: { get: async () => ({ data: fork() }) } } };
  github.rest.repos.getCommit = async () => ({ data: { parents: [{ sha: B }] } });
  github.rest.repos.listPullRequestsAssociatedWithCommit = () => {};
  github.paginate = async (_, args) => args.commit_sha === A ? [{ number: 7, merged_at: '2026-11-17', base: { ref: 'main' } }] : [];
  return { outputs, secrets, calls, core, context, github, botOwner: 'alice' };
}

test('resolve uses configured bot fork and exact labels, independent of merger', async () => {
  for (const manual of [false, true]) {
    const h = harness();
    if (manual) h.github.rest.pulls.get = async () => ({data:{...source(),merged_by:{login:'someone-else',type:'Bot'}}});
    await resolve(h);
    assert.deepEqual(JSON.parse(h.outputs.task), task);
    assert.deepEqual(JSON.parse(h.outputs.targets), ['branch-2.0']);
  }
  const h = harness();
  h.github.rest.pulls.get = async () => ({ data: { ...source(), labels: [{ name: 'branch-2.00' }] } });
  h.github.rest.repos.get = async () => { throw new Error('must not look up an unused fork'); };
  await resolve(h); assert.equal(h.outputs.targets, '[]');
});

test('reject unmerged PRs, unrelated or missing forks, and arbitrary coordinator refs', async () => {
  for (const change of [pr => pr.merged = false, pr => pr.base.ref = 'branch-2.0',
    pr => pr.base.repo.full_name = 'other/repo', pr => pr.merge_commit_sha = 'bad']) {
    const h = harness(), pr = source(); change(pr); h.github.rest.pulls.get = async () => ({ data: pr });
    await assert.rejects(resolve(h)); assert.equal(h.outputs.task, undefined);
  }
  for (const change of [repo => repo.fork = false, repo => repo.private = true, repo => repo.archived = true,
    repo => repo.source.full_name = 'other/repo', repo => repo.owner.login = 'other']) {
    const h = harness(), repo = fork(); change(repo); h.github.rest.repos.get = async () => ({ data: repo });
    await assert.rejects(resolve(h));
  }
  const h = harness(); h.github.rest.repos.get = async () => { throw Object.assign(new Error(), { status: 404 }); };
  await assert.rejects(resolve(h), /create a public gravitino fork/);
  h.context.ref = 'refs/heads/unreviewed'; await assert.rejects(resolve(h), /default branch/);
});

test('bot credential must match configured identity and push the verified fork', async () => {
  const h = harness();
  h.github.rest.users = {getAuthenticated: async () => ({data:{login:'alice'}})};
  h.github.rest.repos.get = async () => ({data:{id:42,permissions:{push:true}}});
  await verifyBot({...h,task});
  h.github.rest.users.getAuthenticated = async () => ({data:{login:'merger'}});
  await assert.rejects(verifyBot({...h,task}), /must belong/);
  h.github.rest.users.getAuthenticated = async () => ({data:{login:'alice'}});
  for (const data of [{id:42,permissions:{push:false}},{id:43,permissions:{push:true}}]) {
    h.github.rest.repos.get = async () => ({data});
    await assert.rejects(verifyBot({...h,task}), /cannot push/);
  }
  for (const botOwner of [' ', 'apache', 'bad/owner']) await assert.rejects(resolve({...h,botOwner}), /Configure/);
});

test('workflow_run discovers only a merged main PR matching the signal head', async () => {
  const h = harness();
  h.context.eventName = 'workflow_run';
  h.context.payload = {repository:{default_branch:'main'},workflow_run:{event:'pull_request',conclusion:'success',head_sha:A}};
  const merged = {...source(),merged_at:'2026-11-18',head:{sha:A}};
  h.github.paginate = async (_, args) => args.commit_sha === A ? [merged] : [];
  await resolve(h);
  assert.deepEqual(JSON.parse(h.outputs.task), task);
  for (const candidate of [{...merged,merged_at:null},{...merged,head:{sha:B}},
    {...merged,base:{...merged.base,ref:'branch-2.0'}}]) {
    h.github.paginate = async () => [candidate];
    await resolve(h); assert.equal(h.outputs.targets, '[]');
  }
  h.github.paginate = async () => [merged, {...merged,number:8}];
  await assert.rejects(resolve(h), /Ambiguous/);
  for (const run of [{event:'push',conclusion:'success',head_sha:A},
    {event:'pull_request',conclusion:'failure',head_sha:A},
    {event:'pull_request',conclusion:'success',head_sha:'invalid'}]) {
    h.context.payload.workflow_run = run;
    await assert.rejects(resolve(h), /Invalid PR signal/);
  }
});

function git(cwd, ...args) { return execFileSync('git', args, { cwd, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] }).trim(); }
function fixture(t) {
  const root = mkdtempSync(path.join(tmpdir(), 'backport-test-')); t.after(() => rmSync(root, { recursive: true, force: true }));
  const upstream = path.join(root, 'upstream'), directory = path.join(root, 'work'); mkdirSync(upstream); mkdirSync(directory);
  git(upstream, 'init', '-b', 'main'); git(upstream, 'config', 'user.name', 'test'); git(upstream, 'config', 'user.email', 'test@example.org');
  writeFileSync(path.join(upstream, 'data.txt'), 'base\n'); git(upstream, 'add', '.'); git(upstream, 'commit', '-m', 'base');
  const baseSha = git(upstream, 'rev-parse', 'HEAD');
  writeFileSync(path.join(upstream, 'data.txt'), 'source\n'); git(upstream, 'commit', '-am', 'source');
  const sourceSha = git(upstream, 'rev-parse', 'HEAD');
  return { root, upstream, directory, baseSha, sourceSha };
}

test('real Git clean cherry-pick preserves content and source provenance', t => {
  const f = fixture(t), result = buildCommit(f);
  assert.equal(result.conflict, false); assert.equal(readFileSync(path.join(f.directory, 'data.txt'), 'utf8'), 'source\n');
  assert.match(git(f.directory, 'log', '-1', '--format=%B'), new RegExp(`Backport-of: ${f.sourceSha}`));
});

test('real Git conflicts are retained, while non-conflict failures and empty patches fail', t => {
  const f = fixture(t); git(f.upstream, 'checkout', '-b', 'release', f.baseSha);
  writeFileSync(path.join(f.upstream, 'data.txt'), 'release\n'); git(f.upstream, 'commit', '-am', 'release');
  const result = buildCommit({ ...f, baseSha: git(f.upstream, 'rev-parse', 'HEAD') });
  assert.equal(result.conflict, true); assert.match(readFileSync(path.join(f.directory, 'data.txt'), 'utf8'), /<<<<<<< HEAD/);
  const empty = path.join(f.root, 'empty'); mkdirSync(empty);
  assert.throws(() => buildCommit({ ...f, directory: empty, baseSha: f.sourceSha }), /empty/);
  const missing = path.join(f.root, 'missing'); mkdirSync(missing);
  assert.throws(() => buildCommit({ ...f, directory: missing, sourceSha: A }), /fetch failed/);
});

test('real Git handles a non-squash merge using the first parent', t => {
  const f = fixture(t); git(f.upstream, 'checkout', '-b', 'feature');
  writeFileSync(path.join(f.upstream, 'other.txt'), 'feature\n'); git(f.upstream, 'add', '.'); git(f.upstream, 'commit', '-m', 'feature');
  git(f.upstream, 'checkout', 'main'); git(f.upstream, 'merge', '--no-ff', 'feature', '-m', 'merge');
  const result = buildCommit({ ...f, baseSha: f.sourceSha, sourceSha: git(f.upstream, 'rev-parse', 'HEAD') });
  assert.equal(result.conflict, false); assert.equal(readFileSync(path.join(f.directory, 'other.txt'), 'utf8'), 'feature\n');
});

test('prepare pushes to a real local fork, reuses partial success, and preserves human edits', async t => {
  const f = fixture(t), bare = path.join(f.root, 'fork.git'), config = path.join(f.root, 'gitconfig');
  git(f.root, 'init', '--bare', bare);
  writeFileSync(config, `[url "${f.upstream}"]\n insteadOf = https://github.com/apache/gravitino.git\n[url "${bare}"]\n insteadOf = https://github.com/alice/gravitino.git\n`);
  const previous = { global: process.env.GIT_CONFIG_GLOBAL, protocol: process.env.GIT_ALLOW_PROTOCOL };
  process.env.GIT_CONFIG_GLOBAL = config; process.env.GIT_ALLOW_PROTOCOL = 'file';
  t.after(() => { for (const [name, value] of [['GIT_CONFIG_GLOBAL', previous.global], ['GIT_ALLOW_PROTOCOL', previous.protocol]]) {
    if (value === undefined) delete process.env[name]; else process.env[name] = value;
  } });
  const h = harness(), branch = 'backport/pr-7/branch-2.0'; let existing;
  h.github.rest.repos.getBranch = async () => ({ data: { commit: { sha: f.baseSha } } });
  h.github.rest.repos.getContent = async () => ({ data: { encoding: 'base64', content: Buffer.from(ROUTING_MARKER).toString('base64') } });
  h.github.rest.actions = { getWorkflow: async () => ({ data: { state: 'active' } }) };
  h.github.rest.git = { getRef: async () => { if (!existing) throw Object.assign(new Error(), { status: 404 }); return { data: { object: { sha: existing } } }; } };
  const args = { ...h, task: { ...task, sha: f.sourceSha, sourceBaseSha: f.baseSha }, target: 'branch-2.0', token: 'test-only-token' };
  await prepare(args); existing = git(bare, 'rev-parse', `refs/heads/${branch}`);
  assert.equal(h.outputs.sha, existing); assert.equal(h.outputs.conflict, 'false');
  await prepare(args); assert.equal(git(bare, 'rev-parse', `refs/heads/${branch}`), existing);
  git(f.directory, 'clone', bare, 'edit'); const edit = path.join(f.directory, 'edit'); git(edit, 'checkout', branch);
  git(edit, 'config', 'user.name', 'human'); git(edit, 'config', 'user.email', 'human@example.org');
  writeFileSync(path.join(edit, 'human.txt'), 'keep'); git(edit, 'add', '.'); git(edit, 'commit', '-m', `Conflicted backport ${f.sourceSha}`); git(edit, 'push', 'origin', branch);
  existing = git(edit, 'rev-parse', 'HEAD'); await prepare(args); assert.equal(h.outputs.sha, existing);
  assert.equal(h.outputs.conflict, 'false', 'a human may retain the original conflict subject after resolving the files');
  // Another actor creates the ref after the API lookup: create-only push must not overwrite it.
  h.github.rest.git.getRef = async () => { throw Object.assign(new Error(), { status: 404 }); };
  await assert.rejects(prepare(args), /git push failed/);
  assert.equal(git(bare, 'rev-parse', `refs/heads/${branch}`), existing);
  h.github.rest.actions.getWorkflow = async () => ({ data: { state: 'disabled_manually' } });
  await assert.rejects(prepare(args), /Enable Fork CI/);
  h.github.rest.actions.getWorkflow = async () => { throw Object.assign(new Error(), { status: 404 }); };
  await assert.rejects(prepare(args), /Sync the fork/);
  h.github.rest.repos.getContent = async () => ({ data: { encoding: 'base64', content: '' } });
  await assert.rejects(prepare(args), /routing support/); assert.equal(git(bare, 'rev-parse', `refs/heads/${branch}`), existing);
});

function prHarness() {
  const h = harness(); let existing = [], created;
  h.github.paginate = async () => existing;
  h.github.rest.pulls.list = () => {};
  h.github.rest.pulls.create = async args => { h.calls.push(args); created = { number: 99, html_url: 'https://example.test/99', state: 'open', labels: [], ...args,
    head: { repo: { full_name: `${args.head.split(':')[0]}/gravitino` } } }; return { data: created }; };
  h.github.rest.issues = { getLabel: async () => ({}), addLabels: async args => h.calls.push(args) };
  return { ...h, existing: value => existing = value, created: () => created };
}

test('PR creation uses fork head; conflict creates a draft and suppresses dispatch; reruns reuse PR', async () => {
  for (const conflict of [false, true]) {
    const h = prHarness(), args = { ...h, task, target: 'branch-2.0', branch: 'backport/pr-7/branch-2.0', conflict };
    await openPullRequest(args); assert.equal(h.calls[0].head, 'alice:backport/pr-7/branch-2.0');
    assert.equal(h.calls[0].draft, conflict); assert.equal(h.outputs.ready, String(!conflict));
    assert.ok(h.calls[0].title.includes(task.title));
    h.existing([h.created()]); await openPullRequest(args); assert.equal(h.calls.filter(c => c.title).length, 1);
    h.existing([{ ...h.created(), state: 'closed' }]); await assert.rejects(openPullRequest(args), /already closed/);
  }
});

test('dispatch checks the current PR and never dispatches duplicate active CI', async () => {
  const h = harness(), pr = { state: 'open', base: { ref: 'branch-2.0' }, head: { sha: A, repo: { full_name: 'alice/gravitino' } }, labels: [] };
  h.github.rest.pulls.get = async () => ({ data: pr });
  h.github.rest.actions = { listWorkflowRuns() {}, createWorkflowDispatch: async args => h.calls.push(args) };
  h.github.paginate = async () => [];
  const args = { ...h, task, target: 'branch-2.0', branch: 'backport/pr-7/branch-2.0', sha: A, number: 99 };
  await dispatch(args); assert.deepEqual(h.calls, [{ owner: 'alice', repo: 'gravitino', workflow_id: 'fork-ci.yml', ref: args.branch }]);
  h.github.paginate = async () => [{ head_branch: args.branch, status: 'queued' }]; await dispatch(args); assert.equal(h.calls.length, 1);
  pr.head.sha = B; await assert.rejects(dispatch(args), /changed/); pr.head.sha = A;
  pr.draft = true; await assert.rejects(dispatch(args), /not ready/); pr.draft = false;
  pr.labels = [{ name: 'cherry-pick-conflict' }]; await assert.rejects(dispatch(args), /not ready/);
});

test('Machine-user backport pushes are lightweight; explicit dispatch remains eligible after opening PR', async () => {
  const h = harness(); const context = { repo: { owner: 'alice', repo: 'gravitino' }, sha: A, ref: 'refs/heads/backport/pr-7/branch-2.0',
    eventName: 'push', payload: { sender: { type: 'User' } } };
  await resolveCI({ ...h, context }); assert.equal(h.outputs.valid, 'false');
  h.github.rest.repos.get = async () => ({ data: fork() });
  h.github.paginate = async () => [{ head: { sha: A, repo: { full_name: 'alice/gravitino' } }, base: { ref: 'branch-2.0' }, labels: [] }];
  h.github.rest.pulls.list = () => {};
  h.github.rest.repos.getBranch = async () => ({ data: { commit: { sha: B } } });
  context.eventName = 'workflow_dispatch'; await resolveCI({ ...h, context });
  assert.equal(h.outputs.valid, 'true'); assert.equal(h.outputs['base-ref'], 'branch-2.0'); assert.equal(h.outputs['head-sha'], A);
  h.github.paginate = async () => [{ head: { sha: A, repo: { full_name: 'alice/gravitino' } }, labels: [{ name: 'cherry-pick-conflict' }] }];
  await resolveCI({ ...h, context }); assert.equal(h.outputs.valid, 'false');
});

// A diagnostic failure must not prevent dispatch after the PR was created successfully.
test('PR summary is best effort, while creation and label errors remain visible', async () => {
  const h = prHarness();
  h.core.summary.write = async () => { throw new Error('summary unavailable'); };
  const args = { ...h, task, target: 'branch-2.0', branch: 'backport/pr-7/branch-2.0', conflict: false };
  await openPullRequest(args); assert.equal(h.outputs.ready, 'true');
  h.github.rest.pulls.create = async () => { throw new Error('API unavailable'); };
  await assert.rejects(openPullRequest(args), /API unavailable/);
  h.existing([h.created()]); h.github.rest.issues.addLabels = async () => { throw new Error('label failure'); };
  await assert.rejects(openPullRequest(args), /label failure/);
});


test('status discovery ignores preparation pushes by Apps and machine users but retains dispatches', async () => {
  const { createService } = require('../fork-ci-status.cjs');
  const branch = 'backport/pr-7/branch-2.0', pr = { number: 99, base: { ref: 'branch-2.0' },
    head: { sha: A, repo: { owner: { login: 'alice' }, name: 'gravitino' } } };
  const ordinary = { id: 1, path: '.github/workflows/fork-ci.yml', head_sha: A, head_branch: branch,
    event: 'workflow_dispatch', actor: { type: 'Bot' } };
  const preparation = { ...ordinary, id: 2, event: 'push' };
  const github = { rest: { actions: { listWorkflowRuns() {} } }, paginate: async () => [ordinary, preparation] };
  assert.equal((await createService(github).findRun(pr, false)).run.id, 1);
  github.paginate = async () => [preparation];
  assert.equal(await createService(github).findRun(pr, false), undefined);
  github.paginate = async () => [{ ...preparation, actor: { type: 'User' } }];
  assert.equal(await createService(github).findRun(pr, false), undefined);
});

test('cancelled preparation runs never retire a previous head status', async () => {
  const { retireCancelled } = require('../retire-fork-ci.cjs');
  const branch = 'backport/pr-7/branch-2.0';
  const github = { rest: { actions: { listWorkflowRuns: async () => ({ data: { workflow_runs: [
    { event: 'push', actor: { type: 'Bot' }, head_branch: branch, head_sha: B, status: 'completed',
      conclusion: 'cancelled', path: '.github/workflows/fork-ci.yml' }
  ] } }) } } };
  // No status-read/write API exists in this fixture: none should be needed.
  await retireCancelled({ github, repo: { owner: 'apache', repo: 'gravitino' }, pr: { head: {
    sha: A, ref: branch, repo: { owner: { login: 'alice' }, name: 'gravitino' } } } });
});


test('rebase merge attribution walks the full change and actual Git backports every commit', async t => {
  const f = fixture(t), first = f.sourceSha;
  writeFileSync(path.join(f.upstream, 'second.txt'), 'second\n'); git(f.upstream, 'add', '.'); git(f.upstream, 'commit', '-m', 'second');
  const last = git(f.upstream, 'rev-parse', 'HEAD'), h = harness();
  h.github.paginate = async (_, args) => [first, last].includes(args.commit_sha)
    ? [{ number: 7, merged_at: '2026-11-17', base: { ref: 'main' } }] : [];
  h.github.rest.repos.getCommit = async args => ({ data: { parents: [{ sha: args.ref === last ? first : f.baseSha }] } });
  const sourceBaseSha = await mergedChangeBase(h.github, { number: 7, merge_commit_sha: last });
  assert.equal(sourceBaseSha, f.baseSha);
  const result = buildCommit({ ...f, sourceSha: last, sourceBaseSha });
  assert.equal(result.conflict, false);
  assert.equal(readFileSync(path.join(f.directory, 'data.txt'), 'utf8'), 'source\n');
  assert.equal(readFileSync(path.join(f.directory, 'second.txt'), 'utf8'), 'second\n');
});

test('missing source attribution and lookup errors fail closed; merge commits use first parent', async () => {
  const h = harness(); h.github.paginate = async () => [];
  await assert.rejects(mergedChangeBase(h.github, source()), /attribution/);
  h.github.paginate = async () => { throw new Error('GitHub unavailable'); };
  await assert.rejects(mergedChangeBase(h.github, source()), /GitHub unavailable/);
  h.github.paginate = async () => [{ number: 7, merged_at: '2026-11-17', base: { ref: 'main' } }];
  h.github.rest.repos.getCommit = async () => ({ data: { parents: [{ sha: B }, { sha: A }] } });
  assert.equal(await mergedChangeBase(h.github, source()), B);
  h.github.rest.repos.getCommit = async () => ({ data: { parents: [{ sha: B }] } });
  await assert.rejects(mergedChangeBase(h.github, source()), /250 commits/);
});

test('marker-free modify/delete and binary conflicts remain blocked after push-before-PR retry', async t => {
  for (const binary of [false, true]) {
    const f = fixture(t);
    if (binary) {
      git(f.upstream, 'checkout', '--detach', f.baseSha);
      writeFileSync(path.join(f.upstream, 'data.txt'), Buffer.from('base\0binary'));
      git(f.upstream, 'commit', '-am', 'binary base'); f.baseSha = git(f.upstream, 'rev-parse', 'HEAD');
      writeFileSync(path.join(f.upstream, 'data.txt'), Buffer.from('source\0binary'));
      git(f.upstream, 'commit', '-am', 'binary source'); f.sourceSha = git(f.upstream, 'rev-parse', 'HEAD');
    }
    git(f.upstream, 'checkout', '-b', 'release', f.baseSha);
    if (binary) writeFileSync(path.join(f.upstream, 'data.txt'), Buffer.from('release\0binary'));
    else git(f.upstream, 'rm', 'data.txt');
    git(f.upstream, 'commit', '-am', 'release conflict');
    const release = git(f.upstream, 'rev-parse', 'HEAD'), bare = path.join(f.root, 'fork.git');
    git(f.root, 'init', '--bare', bare);
    const config = path.join(f.root, 'config');
    writeFileSync(config, `[url "${f.upstream}"]\n insteadOf = https://github.com/apache/gravitino.git\n[url "${bare}"]\n insteadOf = https://github.com/alice/gravitino.git\n`);
    const old = [process.env.GIT_CONFIG_GLOBAL, process.env.GIT_ALLOW_PROTOCOL];
    process.env.GIT_CONFIG_GLOBAL = config; process.env.GIT_ALLOW_PROTOCOL = 'file';
    try {
      const h = harness(); let existing;
      h.github.rest.repos.getBranch = async () => ({ data: { commit: { sha: release } } });
      h.github.rest.repos.getContent = async () => ({ data: { encoding: 'base64', content: Buffer.from(ROUTING_MARKER).toString('base64') } });
      h.github.rest.actions = { getWorkflow: async () => ({ data: { state: 'active' } }) };
      h.github.rest.git = { getRef: async () => {
        if (!existing) throw Object.assign(new Error(), { status: 404 });
        return { data: { object: { sha: existing } } };
      } };
      const args = { ...h, task: { ...task, sha: f.sourceSha, sourceBaseSha: f.baseSha }, target: 'branch-2.0', token: 'test' };
      await prepare(args);
      assert.equal(h.outputs.conflict, 'true');
      existing = h.outputs.sha;
      await prepare(args);
      assert.equal(h.outputs.conflict, 'true');
      assert.equal(h.outputs.markers, 'false');
      const pr = prHarness();
      await openPullRequest({ ...pr, task, target: 'branch-2.0', branch: h.outputs.branch, conflict: true, markers: false });
      assert.equal(pr.created().draft, true); assert.equal(pr.outputs.ready, 'false');
    } finally {
      for (const [i, name] of ['GIT_CONFIG_GLOBAL', 'GIT_ALLOW_PROTOCOL'].entries()) {
        if (old[i] === undefined) delete process.env[name]; else process.env[name] = old[i];
      }
    }
  }
});

test('retry respects explicit human conflict resolution but still blocks current text markers', async () => {
  const h = prHarness(), args = { ...h, task, target: 'branch-2.0', branch: 'backport/pr-7/branch-2.0', conflict: true };
  await openPullRequest(args);
  h.existing([{ ...h.created(), draft: false, labels: [] }]);
  h.calls.length = 0;
  await openPullRequest(args);
  assert.equal(h.outputs.ready, 'true');
  assert.deepEqual(h.calls[0].labels, ['cherry-pick']);
  await openPullRequest({ ...args, markers: true });
  assert.equal(h.outputs.ready, 'false');
  assert.deepEqual(h.calls.at(-1).labels, ['cherry-pick', 'cherry-pick-conflict']);
});

test('PR labels use upstream credentials while public PR creation uses the bot', async () => {
  const h = prHarness();
  const upstream = {rest:{issues:h.github.rest.issues}};
  h.github.rest.issues = new Proxy({}, {get(){throw new Error('must not label with bot credential');}});
  await openPullRequest({...h,upstream,task,target:'branch-2.0',branch:'backport/pr-7/branch-2.0',conflict:false});
  assert.equal(h.outputs.ready, 'true');
});

test('production coordinator wiring keeps credentials out of PR code and uses the bot', () => {
  const workflow = name => readFileSync(path.join(__dirname, '../../workflows/', name), 'utf8');
  const auto = workflow('auto-cherry-pick.yml'), backport = workflow('cherry-pick-branch.yml');
  assert.match(auto, /workflow_run:/);
  assert.doesNotMatch(auto, /pull_request_target:/);
  assert.match(workflow('fork-ci-status.yml'), /types: \[opened, synchronize, reopened, edited, closed\]/);
  assert.match(auto, /vars.BACKPORT_BOT_OWNER/);
  assert.match(auto, /pull-requests: write/);
  assert.match(auto, /issues: write/);
  assert.match(backport, /verifyBot/);
  assert.match(backport, /upstream: new github.constructor/);
  assert.match(backport, /ref: \$\{\{ github.event.repository.default_branch \}\}/);
  assert.doesNotMatch(backport, /BACKPORT_APP|installationToken/);
});

test('absent and empty bot owner preserve upstream backports without querying a bot fork', async () => {
  for (const botOwner of [undefined, '']) {
    const h = harness();
    h.github.rest.repos.get = async () => {throw new Error('must not require a fork');};
    await resolve({...h,botOwner});
    assert.deepEqual(JSON.parse(h.outputs.task), {owner:'apache',repo:'gravitino',number:7,
      sha:A,sourceBaseSha:B,title:task.title});
    assert.deepEqual(JSON.parse(h.outputs.targets), ['branch-2.0']);
  }
  const h = harness();
  h.github.rest.repos.get = async () => {throw new Error('configured fork unavailable');};
  await assert.rejects(resolve(h), /configured fork unavailable/);
  assert.equal(h.outputs.task, undefined, 'configured bot errors must not select upstream');
});

test('upstream preparation pushes and reuses a real branch without fork routing prerequisites', async t => {
  const f = fixture(t), config = path.join(f.root,'config');
  writeFileSync(config, `[url "${f.upstream}"]\n insteadOf = https://github.com/apache/gravitino.git\n`);
  const old = [process.env.GIT_CONFIG_GLOBAL,process.env.GIT_ALLOW_PROTOCOL];
  process.env.GIT_CONFIG_GLOBAL = config;process.env.GIT_ALLOW_PROTOCOL = 'file';
  t.after(() => {
    for (const [i,key] of ['GIT_CONFIG_GLOBAL','GIT_ALLOW_PROTOCOL'].entries()) {
      if (old[i] === undefined) delete process.env[key];else process.env[key] = old[i];
    }
  });
  const h = harness();let existing;
  h.github.rest.repos.getBranch = async () => ({data:{commit:{sha:f.baseSha}}});
  h.github.rest.repos.getContent = async () => {throw new Error('must not require routing files');};
  h.github.rest.actions = {getWorkflow:async () => {throw new Error('must not require Fork CI');}};
  h.github.rest.git = {getRef:async args => {
    assert.equal(args.owner,'apache');
    if (!existing) throw Object.assign(new Error(),{status:404});
    return {data:{object:{sha:existing}}};
  }};
  const args = {...h,task:{...task,owner:'apache',sha:f.sourceSha,sourceBaseSha:f.baseSha},
    target:'branch-2.0',token:'test-only-token'};
  await prepare(args);
  const branch = `cherry-pick-${f.sourceSha.slice(0,8)}-to-branch-2.0`;
  existing = git(f.upstream,'rev-parse',branch);
  assert.equal(h.outputs.branch,branch);assert.equal(h.outputs.sha,existing);
  assert.equal(h.outputs.conflict,'false');
  await prepare(args);assert.equal(h.outputs.sha,existing);
  assert.equal(git(f.upstream,'show',`${branch}:data.txt`),'source');
});

test('upstream mode creates an Apache PR and describes the Apache CI destination', async () => {
  const h = prHarness();
  await openPullRequest({...h,task:{...task,owner:'apache'},target:'branch-2.0',
    branch:'cherry-pick-aaaaaaaa-to-branch-2.0',conflict:false});
  assert.equal(h.created().head.repo.full_name,'apache/gravitino');
  assert.equal(h.calls[0].head,'apache:cherry-pick-aaaaaaaa-to-branch-2.0');
  assert.match(h.calls[0].body,/CI runs in apache\/gravitino/);
  assert.equal(h.calls[0].maintainer_can_modify,undefined);
  assert.equal(h.outputs.ready,'true');
});

test('upstream dispatch preserves legacy PR workflows and explicitly runs migrated release CI', async () => {
  const h = harness();
  h.github.rest.pulls.get = async () => ({data:{state:'open',base:{ref:'branch-2.0'},
    head:{sha:A,repo:{full_name:'apache/gravitino'}},labels:[]}});
  h.github.rest.repos.getContent = async () => {throw Object.assign(new Error(),{status:404});};
  h.github.rest.actions = {listWorkflowRuns(){},createWorkflowDispatch:async args => h.calls.push(args)};
  h.github.paginate = async () => [];
  const args = {...h,task:{...task,owner:'apache'},target:'branch-2.0',
    branch:'cherry-pick-aaaaaaaa-to-branch-2.0',sha:A,number:99};
  await dispatch(args);assert.deepEqual(h.calls,[]);
  h.github.rest.repos.getContent = async () => ({data:{encoding:'base64',content:Buffer.from('old resolver').toString('base64')}});
  await dispatch(args);assert.deepEqual(h.calls,[]);
  h.github.rest.repos.getContent = async () => ({data:{encoding:'base64',content:Buffer.from(ROUTING_MARKER).toString('base64')}});
  await dispatch(args);
  assert.deepEqual(h.calls,[{owner:'apache',repo:'gravitino',workflow_id:'fork-ci.yml',ref:'branch-2.0',inputs:{'pull-request':'99'}}]);
  h.github.paginate = async (_, params) => {
    assert.equal(params.branch,'branch-2.0');assert.equal(params.head_sha,undefined);
    return [{head_branch:'branch-2.0',display_title:'Trusted Fork CI #99',status:'in_progress'}];
  };
  await dispatch(args);assert.equal(h.calls.length,1);
  h.github.paginate = async () => [{head_branch:'branch-2.0',display_title:'Trusted Fork CI #100',status:'in_progress'}];
  await dispatch(args);assert.equal(h.calls.length,2,'another release PR must not block this one');
  h.github.rest.repos.getContent = async () => {throw new Error('API unavailable');};
  await assert.rejects(dispatch(args),/API unavailable/);assert.equal(h.calls.length,2);
});

test('workflow fallback is selectable without a bot secret and isolates upstream write permissions', () => {
  const workflow = name => readFileSync(path.join(__dirname,'../../workflows/',name),'utf8');
  const call = workflow('auto-cherry-pick.yml'), impl = workflow('cherry-pick-branch.yml');
  const fork = call.slice(call.indexOf('  backport:'),call.indexOf('  backport-upstream:'));
  const upstream = call.slice(call.indexOf('  backport-upstream:'));
  assert.match(fork,/contents: read/);assert.doesNotMatch(fork,/contents: write|actions: write/);
  assert.match(fork,/owner != 'apache'/);assert.match(upstream,/owner == 'apache'/);
  assert.match(upstream,/contents: write/);assert.match(upstream,/actions: write/);
  assert.match(impl,/BACKPORT_BOT_TOKEN:\n\s+required: false/);
  assert.match(impl,/CHERRY_PICK_TOKEN:\n\s+required: false/);
  assert.match(impl,/secrets.CHERRY_PICK_TOKEN \|\| github.token/);
  assert.match(impl,/Verify the dedicated bot credential\n\s+if: fromJSON\(inputs.task\).owner != 'apache'/);
});
