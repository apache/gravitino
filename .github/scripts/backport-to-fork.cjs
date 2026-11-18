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
const { mkdtempSync, rmSync } = require('node:fs');
const { tmpdir } = require('node:os');
const path = require('node:path');
const { spawnSync } = require('node:child_process');
const UPSTREAM = { owner: 'apache', repo: 'gravitino' };
const ROUTING_MARKER = 'Backport fork routing v1';
const TARGETS = ['branch-1.3', 'branch-2.0'];

// For a rebase merge, merge_commit_sha names only the last rebased commit.
// GitHub's commit/PR association identifies the merged PR that introduced each
// default-branch commit. Walk its contiguous first-parent range, not commit text.
async function mergedChangeBase(github, pr) {
  let sha = pr.merge_commit_sha;
  const introducedBySource = async commitSha => {
    const prs = await github.paginate(github.rest.repos.listPullRequestsAssociatedWithCommit,
      { ...UPSTREAM, commit_sha: commitSha, per_page: 100 });
    return prs.some(candidate => candidate.number === pr.number && candidate.merged_at && candidate.base.ref === 'main');
  };
  if (!await introducedBySource(sha)) throw new Error('Source merge attribution is not available yet; retry the coordinator.');
  for (let count = 0; count < 250; count++) {
    const { data: commit } = await github.rest.repos.getCommit({ ...UPSTREAM, ref: sha });
    const parent = commit.parents[0]?.sha;
    if (!/^[0-9a-f]{40}$/.test(parent || '')) throw new Error('Cannot identify the parent of the merged change.');
    if (commit.parents.length > 1 || !await introducedBySource(parent)) return parent;
    sha = parent;
  }
  throw new Error('Merged change exceeds 250 commits; prepare this backport manually.');
}

async function resolve({ github, context, core, botOwner }) {
  if (`${context.repo.owner}/${context.repo.repo}` !== 'apache/gravitino') throw new Error('Run backports in apache/gravitino.');
  const defaultBranch = context.payload.repository.default_branch;
  if (context.ref !== `refs/heads/${defaultBranch}`) throw new Error('Run the coordinator from the default branch.');
  let number = context.payload.inputs?.['pull-request'];
  if (context.eventName === 'workflow_run') {
    const run = context.payload.workflow_run;
    if (run.event !== 'pull_request' || run.conclusion !== 'success' ||
        !/^[0-9a-f]{40}$/.test(run.head_sha || '')) throw new Error('Invalid PR signal.');
    const candidates = await github.paginate(github.rest.repos.listPullRequestsAssociatedWithCommit,
      { ...UPSTREAM, commit_sha: run.head_sha, per_page: 100 });
    const merged = candidates.filter(pr => pr.merged_at && pr.base.ref === 'main' &&
      pr.base.repo.full_name === 'apache/gravitino' && pr.head.sha === run.head_sha);
    if (!merged.length) { core.setOutput('targets', '[]'); return; }
    if (merged.length !== 1) throw new Error('Ambiguous PR signal; retry with an explicit source PR number.');
    number = merged[0].number;
  }
  if (!/^[1-9][0-9]*$/.test(String(number))) throw new Error('A source PR number is required.');
  const { data: pr } = await github.rest.pulls.get({ ...UPSTREAM, pull_number: Number(number) });
  if (!pr.merged || pr.base.ref !== 'main' || pr.base.repo.full_name !== 'apache/gravitino' ||
      !/^[0-9a-f]{40}$/.test(pr.merge_commit_sha || '')) throw new Error('The source PR must be merged into apache/gravitino main.');
  const targets = TARGETS.filter(target => pr.labels.some(label => label.name === target));
  core.setOutput('targets', JSON.stringify(targets));
  if (!targets.length) return;
  const owner = botOwner || UPSTREAM.owner;
  if (!botOwner) {
    core.setOutput('task', JSON.stringify({ ...UPSTREAM, number: pr.number,
      sha: pr.merge_commit_sha, sourceBaseSha: await mergedChangeBase(github, pr), title: pr.title }));
    core.notice('BACKPORT_BOT_OWNER is unset; backport branches and CI remain in apache/gravitino.');
    return;
  }
  if (!/^[A-Za-z0-9](?:[A-Za-z0-9-]{0,37}[A-Za-z0-9])?$/.test(owner || '') || owner.toLowerCase() === 'apache') {
    throw new Error('Configure BACKPORT_BOT_OWNER with the dedicated fork account.');
  }
  let fork;
  try { fork = (await github.rest.repos.get({ owner, repo: 'gravitino' })).data; }
  catch (error) {
    if (error.status !== 404) throw error;
    throw new Error(`${owner} must create a public gravitino fork and enable Actions before retrying this workflow.`);
  }
  if (!fork.fork || fork.source?.full_name !== 'apache/gravitino' || fork.private || fork.archived ||
      fork.owner.login.toLowerCase() !== owner.toLowerCase()) throw new Error(`${owner}/gravitino must be an active public fork of apache/gravitino.`);
  const task = { owner, repo: 'gravitino', repositoryId: fork.id, number: pr.number,
    sha: pr.merge_commit_sha, sourceBaseSha: await mergedChangeBase(github, pr), title: pr.title };
  core.setOutput('task', JSON.stringify(task));
}

async function verifyBot({ github, task }) {
  const { data: user } = await github.rest.users.getAuthenticated();
  if (user.login.toLowerCase() !== task.owner.toLowerCase()) throw new Error('BACKPORT_BOT_TOKEN must belong to BACKPORT_BOT_OWNER.');
  const { data: fork } = await github.rest.repos.get({ owner: task.owner, repo: task.repo });
  if (fork.id !== task.repositoryId || !fork.permissions?.push) throw new Error('BACKPORT_BOT_TOKEN cannot push the verified fork.');
}

function git(directory, args, env = {}, allowFailure = false) {
  const result = spawnSync('git', args, { cwd: directory, env: { ...process.env, ...env }, encoding: 'utf8' });
  if (result.error) throw result.error;
  if (result.status !== 0 && !allowFailure) throw new Error(`git ${args[0]} failed: ${result.stderr}`);
  return result;
}

// Only Git reads the fetched trees. Never execute code from the release or backport branch.
function buildCommit({ directory, upstream, baseSha, sourceSha, sourceBaseSha }) {
  git(directory, ['init', '--quiet']);
  git(directory, ['config', 'user.name', 'Apache Gravitino Backport']);
  git(directory, ['config', 'user.email', 'dev@gravitino.apache.org']);
  git(directory, ['fetch', '--no-tags', upstream, baseSha, sourceSha, ...(sourceBaseSha ? [sourceBaseSha] : [])]);
  git(directory, ['checkout', '--detach', baseSha]);
  const parent = sourceBaseSha || git(directory, ['rev-parse', `${sourceSha}^1`]).stdout.trim();
  const tree = git(directory, ['rev-parse', `${sourceSha}^{tree}`]).stdout.trim();
  // One synthetic commit represents the complete integrated change for squash,
  // merge-commit and rebase merges alike; it is never pushed as a source ref.
  const change = git(directory, ['commit-tree', tree, '-p', parent, '-m', `Integrated change ${sourceSha}`]).stdout.trim();
  const picked = git(directory, ['cherry-pick', '--no-commit', change], {}, true);
  const conflicts = git(directory, ['diff', '--name-only', '--diff-filter=U']).stdout.trim();
  if (picked.status !== 0 && !conflicts) throw new Error(`Cherry-pick failed without merge conflicts: ${picked.stderr}`);
  git(directory, ['add', '-A']);
  if (!conflicts && !git(directory, ['diff', '--cached', '--name-only']).stdout.trim()) throw new Error('Cherry-pick is empty; verify whether the change is already backported.');
  git(directory, ['commit', '--allow-empty', '-m', `Backport ${sourceSha}\n\nBackport-of: ${sourceSha}\nBackport-conflicts: ${!!conflicts}`]);
  return { conflict: !!conflicts, sha: git(directory, ['rev-parse', 'HEAD']).stdout.trim() };
}

async function prepare({ github, core, task, target, token }) {
  if (!TARGETS.includes(target) || !/^[0-9a-f]{40}$/.test(task.sha) || !/^[0-9a-f]{40}$/.test(task.sourceBaseSha)) throw new Error('Invalid backport target or source SHA.');
  const upstreamMode = task.owner === UPSTREAM.owner;
  const branch = upstreamMode ? `cherry-pick-${task.sha.slice(0, 8)}-to-${target}` : `backport/pr-${task.number}/${target}`;
  const { data: base } = await github.rest.repos.getBranch({ ...UPSTREAM, branch: target });
  // Roll out routing to the release branch before enabling automated fork pushes.
  for (const file of upstreamMode ? [] : ['.github/scripts/resolve-fork-ci.cjs', '.github/workflows/fork-ci.yml']) {
    let contents;
    try { contents = (await github.rest.repos.getContent({ ...UPSTREAM, ref: base.commit.sha, path: file })).data; }
    catch (error) {
      if (error.status !== 404) throw error;
      throw new Error(`Backport the fork routing support to ${target} first; missing ${file}.`, { cause: error });
    }
    if (contents.encoding !== 'base64' || !Buffer.from(contents.content, 'base64').toString().includes(ROUTING_MARKER)) {
      throw new Error(`Backport the fork routing support to ${target} first; ${file} is not ready. No branch was pushed.`);
    }
  }
  const fork = { owner: task.owner, repo: task.repo };
  if (!upstreamMode) {
    let workflow;
    try { workflow = (await github.rest.actions.getWorkflow({ ...fork, workflow_id: 'fork-ci.yml' })).data; }
    catch (error) {
      if (error.status !== 404) throw error;
      throw new Error(`Sync the fork's default branch and enable Fork CI in ${task.owner}/${task.repo}, then retry.`, { cause: error });
    }
    if (workflow.state !== 'active') throw new Error(`Enable Fork CI in ${task.owner}/${task.repo} before retrying.`);
  }
  let existing;
  try { existing = (await github.rest.git.getRef({ ...fork, ref: `heads/${branch}` })).data; }
  catch (error) { if (error.status !== 404) throw error; }
  const directory = mkdtempSync(path.join(tmpdir(), 'gravitino-backport-'));
  try {
    let result;
    if (existing) {
      git(directory, ['init', '--quiet']);
      git(directory, ['fetch', '--no-tags', 'https://github.com/apache/gravitino.git', base.commit.sha]);
      git(directory, ['fetch', '--no-tags', `https://github.com/${task.owner}/${task.repo}.git`, existing.object.sha]);
      const messages = git(directory, ['log', '-n', '100', '--format=%B%x00', existing.object.sha]).stdout;
      const provenance = messages.split('\0').filter(message => message.split('\n').includes(`Backport-of: ${task.sha}`));
      if (!provenance.length) throw new Error(`Existing ${branch} lacks this source commit's provenance; refusing to overwrite it.`);
      // Reuse the branch, including any human conflict-resolution commits.
      const diff = git(directory, ['diff', '--unified=0', `${base.commit.sha}...${existing.object.sha}`]).stdout;
      const markers = /^\+(<<<<<<< |>>>>>>> |=======\r?$)/m.test(diff);
      // Absence of text markers cannot prove binary or modify/delete conflicts resolved.
      const conflict = provenance.some(message => !message.split('\n').includes('Backport-conflicts: false'));
      result = { sha: existing.object.sha, conflict: conflict || markers, markers };
    } else {
      result = buildCommit({ directory, upstream: 'https://github.com/apache/gravitino.git', baseSha: base.commit.sha, sourceSha: task.sha, sourceBaseSha: task.sourceBaseSha });
      const header = `AUTHORIZATION: basic ${Buffer.from(`x-access-token:${token}`).toString('base64')}`;
      core.setSecret(header);
      git(directory, ['push', '--force-with-lease=refs/heads/' + branch + ':',
        `https://github.com/${task.owner}/${task.repo}.git`, `HEAD:refs/heads/${branch}`],
        { GIT_CONFIG_COUNT: '1', GIT_CONFIG_KEY_0: 'http.https://github.com/.extraheader', GIT_CONFIG_VALUE_0: header });
    }
    for (const [key, value] of Object.entries({ branch, sha: result.sha, conflict: String(result.conflict), markers: String(!!result.markers) })) core.setOutput(key, value);
  } finally { rmSync(directory, { recursive: true, force: true }); }
}

async function openPullRequest({ github, upstream = github, core, task, target, branch, conflict, markers = false }) {
  const prs = await github.paginate(github.rest.pulls.list, { ...UPSTREAM, state: 'all',
    head: `${task.owner}:${branch}`, base: target, per_page: 100 });
  let pr = prs.find(pr => pr.head.repo?.full_name.toLowerCase() === `${task.owner}/${task.repo}`.toLowerCase());
  if (pr && pr.state !== 'open') throw new Error(`Backport PR #${pr.number} is already closed; no CI was dispatched.`);
  // A ready existing PR with its conflict label removed is explicit human acknowledgement.
  const blocked = markers || (conflict && (!pr || pr.draft || pr.labels?.some(label => label.name === 'cherry-pick-conflict')));
  if (!pr) {
    pr = (await github.rest.pulls.create({ ...UPSTREAM, base: target, head: `${task.owner}:${branch}`,
      title: `${conflict ? '[DO NOT MERGE] ' : ''}[Cherry-pick to ${target}] ${task.title}`.slice(0, 256),
      draft: conflict, ...(task.owner === UPSTREAM.owner ? {} : { maintainer_can_modify: true }),
      body: `Backport of #${task.number}.\n\n- Original commit: ${task.sha}\n- Target branch: ${target}\n- Backport fork owner: @${task.owner}\n\n${conflict ? 'Resolve conflicts on this backport branch, remove the cherry-pick-conflict label, mark ready for review, and rerun the backport coordinator for the original PR.' : task.owner === UPSTREAM.owner ? 'CI runs in apache/gravitino.' : 'CI runs in the dedicated bot fork.'}` })).data;
  }
  const labels = ['cherry-pick', ...(blocked ? ['cherry-pick-conflict'] : [])];
  for (const name of labels) {
    try { await upstream.rest.issues.getLabel({ ...UPSTREAM, name }); }
    catch (error) {
      if (error.status !== 404) throw error;
      await upstream.rest.issues.createLabel({ ...UPSTREAM, name, color: name === 'cherry-pick' ? '1D76DB' : 'B60205' });
    }
  }
  await upstream.rest.issues.addLabels({ ...UPSTREAM, issue_number: pr.number, labels });
  const ready = !blocked && !pr.draft && !pr.labels?.some(label => label.name === 'cherry-pick-conflict');
  core.setOutput('ready', String(ready));
  core.setOutput('number', String(pr.number));
  try { await core.summary.addLink(`Backport #${pr.number}`, pr.html_url).write(); }
  catch (error) { core.warning(`Backport #${pr.number} is ready but its job summary could not be written: ${error.message}`); }
}

async function dispatch({ github, core, task, target, branch, sha, number }) {
  const { data: pr } = await github.rest.pulls.get({ ...UPSTREAM, pull_number: Number(number) });
  if (pr.state !== 'open' || pr.draft || pr.base.ref !== target || pr.head.sha !== sha ||
      pr.head.repo?.full_name.toLowerCase() !== `${task.owner}/${task.repo}`.toLowerCase() ||
      pr.labels.some(label => label.name === 'cherry-pick-conflict')) throw new Error('The backport changed or is not ready; retry after resolving it.');
  const upstreamMode = task.owner === UPSTREAM.owner;
  if (upstreamMode) {
    let file;
    try { file = (await github.rest.repos.getContent({ ...UPSTREAM, path: '.github/scripts/resolve-fork-ci.cjs', ref: target })).data; }
    catch (error) { if (error.status !== 404) throw error; }
    if (!file || file.encoding !== 'base64' || !Buffer.from(file.content, 'base64').toString().includes(ROUTING_MARKER)) {
      core.notice('The release branch uses its existing Apache PR workflows; CHERRY_PICK_TOKEN is needed to trigger them automatically.');
      return;
    }
  }
  // A trusted upstream run's head_sha identifies the release workflow, not the
  // tested PR head. Redispatch upstream retries so an older PR-head run cannot
  // suppress current tests; trusted CI concurrency supersedes the previous run.
  if (!upstreamMode) {
    const runs = await github.paginate(github.rest.actions.listWorkflowRuns, {
      owner: task.owner, repo: task.repo, workflow_id: 'fork-ci.yml', head_sha: sha, event: 'workflow_dispatch', per_page: 100 });
    if (runs.some(run => run.head_branch === branch && run.status !== 'completed')) {
      core.notice('Fork CI is already queued or running for this backport.');
      return;
    }
  }
  await github.rest.actions.createWorkflowDispatch({ owner: task.owner, repo: task.repo,
    workflow_id: 'fork-ci.yml', ref: upstreamMode ? target : branch,
    ...(upstreamMode ? { inputs: { 'pull-request': String(number) } } : {}) });
  core.notice(`Started Fork CI in ${task.owner}/${task.repo} for #${number}.`);
}
module.exports = { resolve, mergedChangeBase, verifyBot, buildCommit, prepare, openPullRequest, dispatch, ROUTING_MARKER };
