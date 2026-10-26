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
const { TRUSTED_REQUIRED } = require('./publish-ci-status.cjs');
const UPSTREAM = { owner: 'apache', repo: 'gravitino' };
const SUITES = ['build', 'backend', 'spark', 'flink', 'trino', 'iceberg_rest_trino', 'python',
  'frontend', 'access_control', 'idp_basic', 'mcp', 'maintenance', 'web_ui', 'charts',
  'contrib_catalog', 'asf_allowlist'];
// The fail-fast watcher in fork-ci.yml never fails the run (continue-on-error), but runs that
// started before that change can still fail on the watcher's own API errors.
const FAIL_FAST = 'Stop on first failure';
const FORK_EVENTS = ['push', 'workflow_dispatch'];
const requiredName = (pr, baseSha = pr.base.sha) => `Required CI / ${pr.base.ref} / ${baseSha} / ${pr.head.sha}`;
const MISSING_WORKFLOW = 'Your fork has no Fork CI workflow, so pushes to this branch do not run CI. ' +
  'Merge or rebase the branch onto the latest base branch, which adds .github/workflows/fork-ci.yml, ' +
  'make sure Actions are enabled in your fork, and push again.';
const requiredAction = note => ({ status: 'completed', conclusion: 'action_required',
  title: 'Fork CI needs attention', note });

function evaluate(pr, run, jobs, baseSha = pr.base.sha) {
  const required = jobs.find(job => job.name === requiredName(pr, baseSha));
  if (run.status !== 'completed') {
    // A workflow can remain queued while some of its matrix jobs already have runners.
    const running = run.status === 'in_progress' || jobs.some(job => job.status === 'in_progress');
    return { status: running ? 'in_progress' : 'queued',
      title: running ? 'Fork CI is running' : 'Fork CI is queued, waiting for a runner' };
  }
  const failed = jobs.filter(job => job.name !== FAIL_FAST && ['failure', 'timed_out'].includes(job.conclusion))
    .map(job => job.name);
  // A run that failed only because of the watcher is judged by its suites below.
  const watcherOnly = run.conclusion === 'failure' && !failed.length &&
    jobs.some(job => job.name === FAIL_FAST && ['failure', 'timed_out'].includes(job.conclusion));
  if (run.conclusion !== 'success' && !watcherOnly) {
    // Fork CI cancels its own run on the first failed job, so report that job, not the cancellation.
    if (failed.length) return { status: 'completed', conclusion: 'failure',
      title: `Failed: ${failed[0]}${failed.length > 1 ? ` (+${failed.length - 1} more)` : ''}`.slice(0, 200) };
    return { status: 'completed',
      conclusion: ['failure', 'cancelled', 'timed_out', 'action_required'].includes(run.conclusion)
        ? run.conclusion : 'action_required', title: `Fork CI ${run.conclusion}` };
  }
  if (!required || required.conclusion !== 'success') {
    return requiredAction('The run did not finish Required CI for this exact head, base branch and base SHA. ' +
      'Update the branch and push again, or ask a committer to dispatch a trusted Fork CI run.');
  }
  for (const suite of SUITES) {
    const children = jobs.filter(job => job.name.startsWith(`${suite} / `));
    if (!children.length || !children.some(job => job.conclusion === 'success') ||
        children.some(job => !['success', 'skipped'].includes(job.conclusion))) {
      return requiredAction(`The ${suite} caller did not report successful jobs. Open the run and verify its suites.`);
    }
  }
  return { status: 'completed', conclusion: 'success', title: 'All required suites passed' };
}

function createService(github) {
  const definitions = new Map();
  const trees = new Map();
  async function ciTree(owner, repo, sha) {
    const key = `${owner}/${repo}@${sha}`;
    if (!trees.has(key)) trees.set(key, (async () => {
      const { data: root } = await github.rest.git.getTree({ owner, repo, tree_sha: sha });
      const entries = Object.fromEntries(root.tree.map(e => [e.path, e]));
      const githubTree = entries['.github'];
      const result = { policy: entries['.asf.yaml']?.sha || null };
      if (!githubTree) return result;
      const { data: dirs } = await github.rest.git.getTree({ owner, repo, tree_sha: githubTree.sha });
      for (const name of ['workflows', 'actions', 'scripts']) result[name] = dirs.tree.find(e => e.path === name)?.sha || null;
      return result;
    })());
    return trees.get(key);
  }
  async function unchanged(pr) {
    const key = `${pr.head.repo.full_name}@${pr.head.sha}:${pr.base.sha}`;
    if (definitions.has(key)) return definitions.get(key);
    const [base, head] = await Promise.all([
      ciTree(UPSTREAM.owner, UPSTREAM.repo, pr.base.sha),
      ciTree(pr.head.repo.owner.login, pr.head.repo.name, pr.head.sha)
    ]);
    const same = JSON.stringify(base) === JSON.stringify(head);
    definitions.set(key, same);
    return same;
  }
  async function jobsFor(owner, repo, run) {
    // Partial re-runs retain earlier successful jobs. Take each named job's newest
    // execution, rather than requiring every caller to execute in the latest attempt.
    const jobs = await github.paginate('GET /repos/{owner}/{repo}/actions/runs/{run_id}/jobs',
      { owner, repo, run_id: run.id, filter: 'all', per_page: 100 });
    const latest = new Map();
    for (const job of jobs) {
      if ((job.run_attempt || 1) > run.run_attempt) continue;
      const previous = latest.get(job.name);
      if (!previous || (job.run_attempt || 1) > (previous.run_attempt || 1) ||
          ((job.run_attempt || 1) === (previous.run_attempt || 1) && job.id > previous.id)) {
        latest.set(job.name, job);
      }
    }
    return [...latest.values()];
  }
  async function acceptedBase(pr, sha) {
    if (!sha || sha === pr.base.sha || !/^[0-9a-f]{40}$/.test(sha)) return pr.base.sha;
    const { data } = await github.rest.repos.compareCommitsWithBasehead({ ...UPSTREAM,
      basehead: `${sha}...${pr.base.ref}`, per_page: 1 });
    return ['ahead', 'identical'].includes(data.status) ? sha : pr.base.sha;
  }
  async function testedBase(pr, jobs) {
    // pr.base.sha is not refreshed by later pushes, so runs usually test a newer base commit.
    // Accept that commit only if it is on the PR base branch. The check can stay green after the
    // branch moves on; strict branch protection, not this check, requires an up-to-date branch.
    const prefix = `Required CI / ${pr.base.ref} / `;
    const suffix = ` / ${pr.head.sha}`;
    const job = jobs.find(job => job.name.startsWith(prefix) && job.name.endsWith(suffix));
    const sha = job?.name.slice(prefix.length, -suffix.length);
    return acceptedBase(pr, sha);
  }
  // Trusted runs of every PR share one listing per base branch. A service lives for one
  // publisher pass, so the next pass re-reads it.
  const trustedListings = new Map();
  async function findRun(pr, trusted) {
    const target = trusted ? UPSTREAM : { owner: pr.head.repo.owner.login, repo: pr.head.repo.name };
    // Fork runs start from a push or from "Run workflow" in the fork; both test the fork's own code.
    const list = () => github.paginate(github.rest.actions.listWorkflowRuns, { ...target, workflow_id: 'fork-ci.yml',
      ...(trusted ? { event: 'workflow_dispatch', branch: pr.base.ref } : { head_sha: pr.head.sha }), per_page: 100 });
    if (trusted && !trustedListings.has(pr.base.ref)) trustedListings.set(pr.base.ref, list());
    const all = trusted ? await trustedListings.get(pr.base.ref) : await list();
    const runs = all.filter(run => run.path === '.github/workflows/fork-ci.yml' &&
      (trusted || FORK_EVENTS.includes(run.event)) &&
      (trusted ? run.head_branch === pr.base.ref : run.head_sha === pr.head.sha) &&
      (!trusted || run.display_title === `Trusted Fork CI #${pr.number}`)).sort((a, b) => b.id - a.id);
    return runs[0] && { ...target, run: runs[0], trusted };
  }
  async function mirror(pr) {
    if (!pr.head.repo) return { result: requiredAction('The PR head repository no longer exists.') };
    // Read trusted metadata only. Nothing in the contributor repository is checked out or executed.
    let ordinary;
    let missingWorkflow = false;
    try {
      ordinary = await findRun(pr, false);
    } catch (error) {
      // The fork never ran fork-ci.yml, e.g. the branch predates the Fork CI migration.
      if (error.status !== 404) throw error;
      missingWorkflow = true;
    }
    const sameDefinitions = await unchanged(pr);
    let trusted = await findRun(pr, true);
    let trustedJobs;
    if (trusted) {
      trustedJobs = await jobsFor(trusted.owner, trusted.repo, trusted.run);
      const exact = trustedJobs.some(job => job.name === requiredName(pr, trusted.run.head_sha));
      // The display title identifies only the PR number. A previous contributor head can
      // have a trusted result too; do not let it obscure this head's running fork jobs.
      if (!exact && (trusted.run.status === 'completed' ||
          trustedJobs.some(job => job.name.startsWith('Required CI / ')) ||
          (ordinary && trusted.run.created_at < ordinary.run.created_at))) trusted = undefined;
    }
    let selected = trusted && (!ordinary || (trusted.run.updated_at || trusted.run.created_at) >= (ordinary.run.updated_at || ordinary.run.created_at) || !sameDefinitions)
      ? trusted : ordinary;
    // Run listings can lag behind, e.g. right after a push or during GitHub incidents, so keep
    // waiting instead of asking the author to act.
    if (!selected && missingWorkflow) return { result: { ...requiredAction(MISSING_WORKFLOW),
      title: 'Fork CI has never run in your fork; merge the latest base branch and push' } };
    if (!selected) return { result: { status: 'queued', title: 'Waiting for Fork CI; enable Actions in your fork if no run starts',
      note: 'No Fork CI run was found for this commit yet. If none starts, enable Actions in your fork and push again, ' +
        'or ask a committer to dispatch trusted Fork CI.' } };
    const { owner, repo, run } = selected;
    // A manual upstream run uses the workflow definitions at run.head_sha, not the PR definitions.
    const jobs = selected.trusted ? trustedJobs : await jobsFor(owner, repo, run);
    const base = selected.trusted ? run.head_sha : !sameDefinitions ? pr.base.sha : await testedBase(pr, jobs);
    let result = evaluate(pr, run, jobs, base);
    if (selected.trusted && result.conclusion === 'success' && await acceptedBase(pr, base) !== base) {
      result = requiredAction('Trusted CI tested a commit outside the current base branch. Dispatch trusted Fork CI again from the current default branch.');
    }
    if (!selected.trusted && !sameDefinitions) {
      const note = 'CI workflow/action/script definitions or branch protection differ from the base. ' +
        'After reviewing these changes, a committer must dispatch Fork CI from the current upstream default branch with this PR number.';
      // Approval of CI edits is separate from execution progress. Never turn a running fork
      // into a completed error, or hide an actual failure behind the trust requirement.
      result = run.status === 'completed' && run.conclusion === 'success'
        ? { ...requiredAction(note), title: TRUSTED_REQUIRED }
        : { ...result, note: `${result.title}. ${note}` };
    }
    return { ...selected, fork: ordinary?.run, base, result };
  }
  return { mirror, unchanged, findRun };
}
module.exports = { FAIL_FAST, FORK_EVENTS, SUITES, requiredName, evaluate, createService };
