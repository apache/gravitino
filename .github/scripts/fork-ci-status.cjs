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
const SUITES = ['build', 'backend', 'spark', 'flink', 'trino', 'iceberg_rest_trino', 'python',
  'frontend', 'access_control', 'idp_basic', 'mcp', 'maintenance', 'web_ui', 'charts',
  'contrib_catalog', 'asf_allowlist'];
const requiredName = (pr, baseSha = pr.base.sha) => `Required CI / ${pr.base.ref} / ${baseSha} / ${pr.head.sha}`;
const requiredAction = note => ({ status: 'completed', conclusion: 'action_required',
  title: 'Fork CI needs attention', note });

function evaluate(pr, run, jobs, baseSha = pr.base.sha) {
  const required = jobs.find(job => job.name === requiredName(pr, baseSha));
  if (run.status !== 'completed') return { status: 'in_progress', title: 'Fork CI is running' };
  if (run.conclusion !== 'success') {
    // Fork CI cancels its own run on the first failed job, so report that job, not the cancellation.
    const failed = jobs.filter(job => ['failure', 'timed_out'].includes(job.conclusion)).map(job => job.name);
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
  async function testedBase(pr, jobs) {
    // pr.base.sha is not refreshed by later pushes, so runs usually test a newer base commit.
    // Accept that commit only if it is on the PR base branch. The check can stay green after the
    // branch moves on; strict branch protection, not this check, requires an up-to-date branch.
    const prefix = `Required CI / ${pr.base.ref} / `;
    const suffix = ` / ${pr.head.sha}`;
    const job = jobs.find(job => job.name.startsWith(prefix) && job.name.endsWith(suffix));
    const sha = job?.name.slice(prefix.length, -suffix.length);
    if (!sha || sha === pr.base.sha || !/^[0-9a-f]{40}$/.test(sha)) return pr.base.sha;
    const { data } = await github.rest.repos.compareCommitsWithBasehead({ ...UPSTREAM,
      basehead: `${sha}...${pr.base.ref}`, per_page: 1 });
    return ['ahead', 'identical'].includes(data.status) ? sha : pr.base.sha;
  }
  async function findRun(pr, trusted) {
    const target = trusted ? UPSTREAM : { owner: pr.head.repo.owner.login, repo: pr.head.repo.name };
    const all = await github.paginate(github.rest.actions.listWorkflowRuns, { ...target, workflow_id: 'fork-ci.yml',
      event: trusted ? 'workflow_dispatch' : 'push', head_sha: trusted ? pr.base.sha : pr.head.sha, per_page: 100 });
    const runs = all.filter(run => run.path === '.github/workflows/fork-ci.yml' &&
      run.head_sha === (trusted ? pr.base.sha : pr.head.sha) &&
      (!trusted || run.display_title === `Trusted Fork CI #${pr.number}`)).sort((a, b) => b.id - a.id);
    return runs[0] && { ...target, run: runs[0], trusted };
  }
  async function mirror(pr) {
    if (!pr.head.repo) return { result: requiredAction('The PR head repository no longer exists.') };
    // Read trusted metadata only. Nothing in the contributor repository is checked out or executed.
    let ordinary = await findRun(pr, false);
    const sameDefinitions = await unchanged(pr);
    const trusted = await findRun(pr, true);
    let selected = trusted && (!ordinary || (trusted.run.updated_at || trusted.run.created_at) >= (ordinary.run.updated_at || ordinary.run.created_at) || !sameDefinitions)
      ? trusted : ordinary;
    if (!selected) return { result: requiredAction('Enable Actions in the fork and push the branch, or dispatch trusted Fork CI.') };
    const { owner, repo, run } = selected;
    if (!selected.trusted && !sameDefinitions) return { ...selected, fork: ordinary?.run,
      result: requiredAction('CI workflow/action/script definitions or branch protection differ from the base. ' +
        'A committer must dispatch Fork CI from the current upstream default branch with this PR number.') };
    // A manual upstream run uses the workflow definitions at run.head_sha, not the PR definitions.
    const jobs = await jobsFor(owner, repo, run);
    const base = selected.trusted ? pr.base.sha : await testedBase(pr, jobs);
    return { ...selected, fork: ordinary?.run, base, result: evaluate(pr, run, jobs, base) };
  }
  return { mirror, unchanged, findRun };
}
module.exports = { SUITES, requiredName, evaluate, createService };
