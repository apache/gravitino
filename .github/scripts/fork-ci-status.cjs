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
const { isBackportPreparation } = require('./resolve-fork-ci.cjs');
const { TRUSTED_REQUIRED, NEEDS_ATTENTION } = require('./publish-ci-status.cjs');
const { readFileSync } = require('node:fs');
const path = require('node:path');
const UPSTREAM = { owner: 'apache', repo: 'gravitino' };

// Only the block-list syntax used by Required CI is accepted. Unsupported workflow
// changes must fail closed rather than silently dropping required callers. SUITES below
// parses the default branch's fork-ci.yml when this module loads, so an unsupported
// format there stops the publisher for every PR; the script tests parse it first.
function requiredSuites(source) {
  const lines = source.split(/\r?\n/);
  const starts = lines.flatMap((line, i) => /^  required_ci:/.test(line) ? [i] : []);
  if (starts.length !== 1 || lines[starts[0]].trim() !== 'required_ci:') {
    throw new Error('Expected one required_ci job in Fork CI workflow');
  }
  const block = [];
  for (const line of lines.slice(starts[0] + 1)) {
    if (/^\s*(#.*)?$/.test(line)) continue;
    if (/^\S|^ {1,2}\S/.test(line)) break;
    block.push(line);
  }
  const needs = block.flatMap((line, i) => /^    needs:/.test(line) ? [i] : []);
  if (needs.length !== 1 || block[needs[0]].trim() !== 'needs:') {
    throw new Error('Expected a block-list of required_ci.needs');
  }
  const names = [];
  for (const line of block.slice(needs[0] + 1)) {
    if (/^ {0,4}\S/.test(line)) break;
    const match = /^      - ([a-zA-Z_][a-zA-Z0-9_-]*)[ \t]*$/.exec(line);
    if (!match) throw new Error('Unsupported required_ci.needs entry');
    names.push(match[1]);
  }
  if (!names.includes('eligibility') || names.length < 2 || new Set(names).size !== names.length) {
    throw new Error('Invalid required_ci.needs callers');
  }
  return names.filter(name => name !== 'eligibility');
}
const SUITES = requiredSuites(readFileSync(path.join(__dirname, '../workflows/fork-ci.yml'), 'utf8'));
// The fail-fast watcher in fork-ci.yml never fails the run (continue-on-error), but runs that
// started before that change can still fail on the watcher's own API errors.
const FAIL_FAST = 'Stop on first failure';
const FORK_EVENTS = ['push', 'workflow_dispatch'];
const requiredName = (pr, baseSha = pr.base.sha) => `Required CI / ${pr.base.ref} / ${baseSha} / ${pr.head.sha}`;
const MISSING_WORKFLOW = 'Your fork has no Fork CI workflow, so pushes to this branch do not run CI. ' +
  'Merge or rebase the branch onto the latest base branch, which adds .github/workflows/fork-ci.yml, ' +
  'make sure Actions are enabled in your fork, and push again.';
const requiredAction = note => ({ status: 'completed', conclusion: 'action_required',
  title: NEEDS_ATTENTION, note });

function incompleteAttempt(run, jobs) {
  if (jobs.some(job => job.run_attempt > run.run_attempt)) {
    return requiredAction('The jobs API reports a newer attempt than the run listing. ' +
      'Waiting for consistent results; the publisher will automatically check again.');
  }
  // A completed rerun must expose at least one job from that attempt. Otherwise all
  // retained successes (and auxiliary cancellations) could belong to the previous run.
  if (run.status === 'completed' && run.run_attempt > 1 &&
      !jobs.some(job => job.run_attempt === run.run_attempt)) {
    return requiredAction('The jobs API has not reported the completed current attempt yet. ' +
      'Waiting for consistent results; the publisher will automatically check again.');
  }
}

function evaluateExecution(pr, run, jobs, baseSha) {
  const incomplete = incompleteAttempt(run, jobs);
  if (incomplete) return incomplete;
  const required = jobs.find(job => job.name === requiredName(pr, baseSha));
  if (run.status !== 'completed') {
    // A workflow can remain queued while some of its matrix jobs already have runners.
    const running = run.status === 'in_progress' || jobs.some(job => job.status === 'in_progress');
    return { status: running ? 'in_progress' : 'queued',
      title: running ? 'Fork CI is running' : 'Fork CI is queued, waiting for a runner' };
  }
  // A nonmatching summary is obsolete only when it is provably from an older attempt.
  // A newer (possibly unexpanded) summary must not reuse an earlier exact success.
  const otherSummary = job => job.name.startsWith('Required CI / ') && job !== required;
  const obsoleteSummary = job => otherSummary(job) && required &&
    Number.isInteger(job.run_attempt) && job.run_attempt > 0 &&
    Number.isInteger(required.run_attempt) && required.run_attempt > job.run_attempt;
  const isAuxiliary = job => job.name === FAIL_FAST || obsoleteSummary(job);
  const currentJobs = jobs.filter(job => !isAuxiliary(job));
  // A previous attempt's unsuccessful job may be awaiting its replacement in the jobs API.
  // It cannot become a settled failure for this rerun. Current failures remain authoritative.
  const fromCurrentAttempt = job => run.run_attempt === 1 || job.run_attempt === run.run_attempt;
  const failed = currentJobs.filter(job => !otherSummary(job) && fromCurrentAttempt(job) &&
    ['failure', 'timed_out'].includes(job.conclusion)).map(job => job.name);
  // Partial reruns can retain a cancelled watcher or an obsolete Required CI job, keeping
  // the whole run cancelled after its suites recover. Ignore only those auxiliary results,
  // then verify the exact Required CI and all required suites below. Any other unfinished
  // or unsuccessful job still blocks.
  const auxiliaryOnly = ['failure', 'timed_out', 'cancelled'].includes(run.conclusion) &&
    jobs.some(job => isAuxiliary(job) && ['failure', 'timed_out', 'cancelled'].includes(job.conclusion)) &&
    currentJobs.every(job => ['success', 'skipped'].includes(job.conclusion));
  // Run and job metadata are separate API snapshots. Even a successful aggregate must
  // not hide a failed prerequisite or suite observed in the jobs response.
  if (failed.length) return { status: 'completed', conclusion: 'failure',
    title: `Failed: ${failed[0]}${failed.length > 1 ? ` (+${failed.length - 1} more)` : ''}`.slice(0, 200) };
  if (jobs.some(job => otherSummary(job) && !obsoleteSummary(job))) {
    return requiredAction('A Required CI record from the same, newer or unknown attempt does not match ' +
      'this head and base. Rerun Required CI for the current context before publishing success.');
  }
  const stale = currentJobs.find(job => !fromCurrentAttempt(job) &&
    !['success', 'skipped'].includes(job.conclusion));
  if (stale) {
    return requiredAction(`The result for ${stale.name} is not from the current attempt. ` +
      'Waiting for consistent results; the publisher will automatically check again.');
  }
  // Seeing one current job does not establish that downstream results are complete.
  // Overriding an unsuccessful aggregate requires a successful gate in THIS attempt.
  // A watcher-only rerun with an older gate must rerun Required CI too: the API snapshot
  // alone cannot distinguish it from a rerun whose downstream records have not arrived.
  const gateBehind = required?.conclusion === 'success' &&
    currentJobs.every(job => ['success', 'skipped'].includes(job.conclusion)) && currentJobs.some(job =>
    Number.isInteger(job.run_attempt) && job.run_attempt > 0 &&
    (!Number.isInteger(required.run_attempt) || job.run_attempt > required.run_attempt));
  if (run.run_attempt > 1 && (gateBehind || (run.conclusion !== 'success' &&
      required?.run_attempt !== run.run_attempt &&
      currentJobs.every(job => ['success', 'skipped'].includes(job.conclusion))))) {
    return requiredAction('Required CI has not confirmed the current attempt. The publisher will ' +
      'check again automatically; if only the watcher was rerun, rerun Required CI as well.');
  }
  if (currentJobs.some(job => !job.conclusion)) {
    return requiredAction('The completed run still has unfinished job results. ' +
      'The publisher will check the results again automatically.');
  }
  if (run.conclusion !== 'success' && !auxiliaryOnly) {
    // The aggregate may arrive before its unsuccessful job (including the watcher).
    // Without that evidence a settled cancellation would suppress the next jobs read.
    if (['failure', 'timed_out', 'cancelled'].includes(run.conclusion) &&
        currentJobs.every(job => ['success', 'skipped'].includes(job.conclusion))) {
      return requiredAction('The unsuccessful run has no corresponding unsuccessful job result yet. ' +
        'Waiting for consistent results; the publisher will automatically check again.');
    }
    return { status: 'completed',
      conclusion: ['failure', 'cancelled', 'timed_out', 'action_required'].includes(run.conclusion)
        ? run.conclusion : 'action_required', title: `Fork CI ${run.conclusion}` };
  }
  if (currentJobs.some(job => !['success', 'skipped'].includes(job.conclusion))) {
    return requiredAction('The run reports success but a required job is cancelled or unfinished. ' +
      'The publisher will check the results again automatically.');
  }
  if (!required || required.conclusion !== 'success') {
    return requiredAction('The run did not finish Required CI for this exact head, base branch and base SHA. ' +
      'Update the branch and push again, or ask a committer to dispatch a trusted Fork CI run.');
  }
}

function evaluateSuites(jobs, suites, trusted = false) {
  for (const suite of suites) {
    const children = jobs.filter(job => job.name.startsWith(`${suite} / `));
    if (!children.length) {
      // A trusted run executes the upstream workflow that defines its suites, so only a fork can lack one.
      return requiredAction(trusted ? `The trusted run has no ${suite} jobs. Dispatch trusted Fork CI again.`
        : `The run has no ${suite} jobs. If the base branch added this suite, merge the latest base branch and push.`);
    }
    if (!children.some(job => job.conclusion === 'success') ||
        children.some(job => !['success', 'skipped'].includes(job.conclusion))) {
      return requiredAction(`The ${suite} caller did not report successful jobs. Open the run and verify its suites.`);
    }
  }
  return { status: 'completed', conclusion: 'success', title: 'All required suites passed' };
}

function evaluate(pr, run, jobs, baseSha = pr.base.sha, suites = SUITES) {
  return evaluateExecution(pr, run, jobs, baseSha) || evaluateSuites(jobs, suites);
}

function createService(github, { readWorkflow = sha => github.rest.repos.getContent({ ...UPSTREAM,
  path: '.github/workflows/fork-ci.yml', ref: sha }).then(response => response.data) } = {}) {
  const definitions = new Map();
  const trees = new Map();
  const suitesBySha = new Map();
  async function suitesAt(sha) {
    if (!suitesBySha.has(sha)) suitesBySha.set(sha, (async () => {
      const data = await readWorkflow(sha);
      if (data.encoding !== 'base64' || typeof data.content !== 'string') {
        throw new Error(`Cannot read trusted Fork CI workflow contents at ${sha}; retry the status refresh`);
      }
      return requiredSuites(Buffer.from(data.content, 'base64').toString('utf8'));
    })());
    return suitesBySha.get(sha);
  }
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
      // Keep evidence of a rerun that started after the run listing was read. Silently
      // discarding it could turn an earlier attempt's success into the current result.
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
    try {
      const { data } = await github.rest.repos.compareCommitsWithBasehead({ ...UPSTREAM,
        basehead: `${sha}...${pr.base.ref}`, per_page: 1 });
      return ['ahead', 'identical'].includes(data.status) ? sha : pr.base.sha;
    } catch (error) {
      // A workflow can name a nonexistent SHA. Reject that context without hiding
      // authoritative job failures; transient API errors must still retry the refresh.
      if (error.status !== 404) throw error;
      return pr.base.sha;
    }
  }
  async function testedBase(pr, jobs) {
    // pr.base.sha is not refreshed by later pushes, so runs usually test a newer base commit.
    // Accept that commit only if it is on the PR base branch. The check can stay green after the
    // branch moves on; strict branch protection, not this check, requires an up-to-date branch.
    const prefix = `Required CI / ${pr.base.ref} / `;
    const suffix = ` / ${pr.head.sha}`;
    // A rerun of the whole run repeats eligibility, which can name a newer base commit. jobsFor keeps
    // both names, so take the newest attempt: it selects the tested callers and Required CI result.
    const job = jobs.filter(job => job.name.startsWith(prefix) && job.name.endsWith(suffix))
      .sort((a, b) => (b.run_attempt || 1) - (a.run_attempt || 1) || b.id - a.id)[0];
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
      !isBackportPreparation(run.event, `refs/heads/${run.head_branch}`) &&
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
    let trustedExact = false;
    let trustedCurrentExact = false;
    if (trusted) {
      trustedJobs = await jobsFor(trusted.owner, trusted.repo, trusted.run);
      trustedExact = trustedJobs.some(job => job.name === requiredName(pr, trusted.run.head_sha));
      trustedCurrentExact = trustedJobs.some(job => job.name === requiredName(pr, trusted.run.head_sha) &&
        (job.run_attempt || 1) === trusted.run.run_attempt);
      // A missing or unexpanded gate is unknown context, not evidence of an older PR head.
      // Only discard a run whose current summaries all explicitly identify another head.
      const summaries = trustedJobs.filter(job => job.name.startsWith('Required CI / ') &&
        (job.run_attempt || 1) === trusted.run.run_attempt);
      const otherHead = summaries.length > 0 && summaries.every(job => {
        const match = /^Required CI \/ (.+) \/ ([0-9a-f]{40}) \/ ([0-9a-f]{40})$/.exec(job.name);
        return match && match[3] !== pr.head.sha;
      });
      // Retained gates from earlier attempts do not identify the current attempt's PR head.
      if (otherHead && !incompleteAttempt(trusted.run, trustedJobs)) trusted = undefined;
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
    const incomplete = incompleteAttempt(run, jobs);
    if (incomplete) return { ...selected, fork: ordinary?.run, result: incomplete };
    if (selected.trusted && !trustedExact && run.status === 'completed') {
      return { ...selected, fork: ordinary?.run, result: requiredAction(
        'Trusted CI has not identified this PR head and base. The publisher will check again; ' +
        'if the gate remains missing, dispatch a new trusted run for the current PR.') };
    }
    // CI edits change who may attest success, not which base the run actually tested.
    const base = selected.trusted ? run.head_sha : await testedBase(pr, jobs);
    let result = evaluateExecution(pr, run, jobs, base);
    // A retained exact gate identifies an earlier attempt. Do not settle a newer
    // failure/cancellation until that attempt identifies its head too: a full rerun
    // may have resolved a different PR head whose summary has not arrived yet.
    // Successful watcher-only reruns still reach the suite checks below.
    if (selected.trusted && !trustedCurrentExact && result?.status === 'completed' &&
        result.title !== NEEDS_ATTENTION) {
      result = requiredAction('Trusted CI has not identified this attempt\'s PR head and base. ' +
        'The publisher will check again before attributing its unsuccessful jobs to this PR.');
    }
    const needsSuiteChecks = !result;
    if (needsSuiteChecks) {
      // Require the callers of the commit the run tested: the upstream dispatch commit for trusted
      // runs, or the base-branch commit merged into the head for fork runs. A suite added on the
      // base branch after that commit does not fail the run, but one the tested code already had
      // must have run, even when the head's own workflow predates it.
      result = !selected.trusted && !sameDefinitions ? requiredAction(TRUSTED_REQUIRED)
        : evaluateSuites(jobs, await suitesAt(base), selected.trusted);
    }
    if (selected.trusted && result.conclusion === 'success' && await acceptedBase(pr, base) !== base) {
      result = requiredAction('Trusted CI tested a commit outside the current base branch. Dispatch trusted Fork CI again from the current default branch.');
    }
    if (!selected.trusted && !sameDefinitions) {
      const note = 'CI workflow/action/script definitions or branch protection differ from the base. ' +
        'After reviewing these changes, a committer must dispatch Fork CI from the current upstream default branch with this PR number.';
      // Approval of CI edits is separate from execution progress. Never turn a running fork
      // into a completed error, or hide an actual failure behind the trust requirement.
      result = needsSuiteChecks
        ? { ...requiredAction(note), title: TRUSTED_REQUIRED }
        : { ...result, note: `${result.note || result.title}. ${note}` };
    }
    return { ...selected, fork: ordinary?.run, base, result };
  }
  return { mirror, unchanged, findRun };
}
module.exports = { requiredSuites, FAIL_FAST, FORK_EVENTS, SUITES, requiredName, evaluate, createService };
