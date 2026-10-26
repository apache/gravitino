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
const { createService } = require('./fork-ci-status.cjs');
const { latestStatus, parseRunUrl, publishStatus, runUrl, statusCompleted, statusState } = require('./publish-ci-status.cjs');
const { retireCancelled } = require('./retire-fork-ci.cjs');

const RECENT_MS = 6 * 60 * 60 * 1000;
// Fork runs finish without an upstream event, and GitHub fires five-minute schedules only every
// 10-36 minutes on apache/gravitino. A sweep therefore keeps polling for LOOP_MS, one pass every
// PASS_MS, staying below the 15-minute job limit of ubuntu-slim, and then dispatches the next
// sweep (the rearm job), so polling is continuous; the schedule restarts a broken chain. Sweeps
// run in their own concurrency group, so PR signals never wait behind a sleeping sweep.
const LOOP_MS = 11 * 60 * 1000;
const PASS_MS = 60 * 1000;
// Back-to-back sweeps spend at most this share of the hourly API quota.
const SWEEP_SHARE = 0.4;

async function update({ github, context, core, sleep = ms => new Promise(resolve => setTimeout(resolve, ms)), clock = Date.now }) {
  const trustedSignal = context.eventName === 'workflow_run' &&
    context.payload.workflow_run.event === 'workflow_dispatch' &&
    /^Trusted Fork CI #([1-9][0-9]*)$/.exec(context.payload.workflow_run.display_title || '');
  // Every other workflow_run, a PR signal or a finished Fork CI push run in this repository (for
  // example a same-repository PR branch, or main after a merge), refreshes the PRs with its head
  // once. It must never become a polling sweep: it runs in the signal queue and would block it.
  const signal = context.eventName === 'workflow_run' && !trustedSignal;
  // A sweep dispatched by the previous sweep runs like a scheduled one.
  const chained = context.eventName === 'workflow_dispatch' && String(context.payload.inputs?.sweep) === 'true';
  const manual = (context.eventName === 'workflow_dispatch' && !chained) || !!trustedSignal || signal;
  const requested = (!chained && context.payload.inputs?.['pull-request']) || (trustedSignal && trustedSignal[1]);
  if (requested && !/^[1-9][0-9]*$/.test(requested)) throw new Error('Invalid PR number.');
  async function listPrs() {
    const prs = requested
      ? [(await github.rest.pulls.get({ ...context.repo, pull_number: Number(requested) })).data]
      : await github.paginate(github.rest.pulls.list, { ...context.repo, state: 'open', per_page: 100 });
    return signal ? prs.filter(pr => pr.head.sha === context.payload.workflow_run.head_sha) : prs;
  }
  // Never restore decisions or scheduling state from Actions caches: PR code dispatched
  // on the default branch can write those caches using its runner runtime token.
  let service = createService(github);
  const availableBases = new Map();
  const rate = (await github.rest.rateLimit.get()).data.resources.core;
  // Bound work even while settled successes are watched. Rotate the starting PR to avoid starving
  // older PRs, and leave quota for signal workflows and other repository automation. Organizations on
  // GitHub Enterprise Cloud, such as apache, get 15,000 requests per hour. Sweeps run back to back,
  // so budget them by time (SWEEP_SHARE of the hourly quota), and never less than 60 calls per sweep.
  const perSweep = Math.max(60, Math.floor(rate.limit * SWEEP_SHARE * LOOP_MS / (60 * 60 * 1000)));
  const budget = manual ? Math.max(0, rate.remaining - 100) : Math.min(perSweep, Math.max(0, rate.remaining - 100));
  // Small quotas keep one pass per sweep; larger ones split the sweep budget across passes.
  const passes = manual ? 1 : Math.max(1, Math.min(Math.ceil(LOOP_MS / PASS_MS), Math.floor(budget / 60)));
  const slotMs = passes > 1 ? PASS_MS : 300000;
  // Only polling sweeps continue the chain. With a small quota a sweep is a single pass, and
  // dispatching it back to back would exceed the budget; the schedule paces those instead.
  if (!manual) core.setOutput('chain', String(passes > 1));
  let calls = 0;
  github.hook.before('request', () => { calls++; });
  const failures = new Set();
  const forkStatus = pr => latestStatus({ github, repo: context.repo, sha: pr.head.sha, context: 'Fork CI' });
  async function migrated(pr) {
    // Release branches without this CI migration keep their own pull_request workflows.
    if (!availableBases.has(pr.base.sha)) {
      availableBases.set(pr.base.sha, github.rest.repos.getContent({ ...context.repo,
        path: '.github/workflows/fork-ci.yml', ref: pr.base.sha }).then(() => true, error => {
        if (error.status === 404) return false;
        throw error;
      }));
    }
    return availableBases.get(pr.base.sha);
  }
  // Without a run, link to the fork's Fork CI page, where the author can enable Actions or start one.
  function waitingUrl(pr) {
    return pr.head.repo
      ? `https://github.com/${pr.head.repo.owner.login}/${pr.head.repo.name}/actions/workflows/fork-ci.yml` : pr.html_url;
  }
  // apache/gravitino cannot re-run jobs in a contributor's fork; the status link opens the run.
  function describe(result, mirror) {
    const hint = `${result.title}; re-run it in your fork`;
    return mirror.run && !mirror.trusted && ['failure', 'error'].includes(statusState(result)) && hint.length <= 140
      ? hint : result.title;
  }
  async function refresh(pr) {
    if (!(await migrated(pr))) return;
    refreshed.add(pr.number);
    const mirror = await service.mirror(pr);
    const { result, run } = mirror;
    const target = run ? runUrl(mirror.owner, mirror.repo, run, mirror.trusted && mirror.fork) : waitingUrl(pr);
    // Re-read before publishing so a push or retarget during the API calls cannot get a stale success.
    const { data: current } = await github.rest.pulls.get({ ...context.repo, pull_number: pr.number });
    if (current.head.sha !== pr.head.sha || current.base.sha !== pr.base.sha || current.base.ref !== pr.base.ref || current.state !== 'open') return;
    // Signals and sweeps can publish concurrently. Read the status last and never replace a newer
    // attempt of a run from the same repository, or a completed result with a running one. Any
    // remaining interleaving is repaired by the next pass, which compares the status with the runs.
    const latest = await forkStatus(pr);
    const shown = parseRunUrl(latest?.target_url);
    // A run already shown for this commit still exists; a listing that missed it is incomplete.
    if (!run && shown) return;
    if (run && shown && shown.owner === mirror.owner && shown.repo === mirror.repo && (shown.run_id > run.id ||
        (shown.run_id === run.id && (shown.run_attempt > run.run_attempt ||
          (shown.run_attempt === run.run_attempt && statusCompleted(latest) && result.status !== 'completed'))))) return;
    await publishStatus({ github, repo: context.repo, sha: pr.head.sha, context: 'Fork CI',
      state: statusState(result), description: describe(result, mirror), targetUrl: target, previous: latest ?? null });
  }
  async function visit(pr, work) {
    processed++;
    visited.add(pr.number);
    if (pr.state !== 'open') return;
    try {
      if (!(await migrated(pr))) return;
      await work();
      // Publish the current head first; history cleanup failures must not block its result.
      // A probe that found the check settled skips it: pushes, which cancel previous runs,
      // always signal a refresh, so probes stay cheap enough to reach every recent PR each pass.
      if (refreshed.has(pr.number)) await retireCancelled({ github, repo: context.repo, pr });
      failures.delete(pr.number);
    } catch (error) {
      failures.add(pr.number);
      core.warning(`Could not refresh #${pr.number}: ${error.message}`);
    }
  }
  let processed = 0;
  let visited = new Set();
  let refreshed = new Set();
  let prs = [];
  let recentCursor = null;
  async function pass(budget) {
    const now = clock();
    prs = await listPrs();
    processed = 0;
    visited = new Set();
    refreshed = new Set();
    service = createService(github);
    // Fork runs, including reruns, finish without an upstream event. Such PRs were updated recently,
    // so probe those first. A completed result changes only with a push or a trusted run, which both
    // signal a refresh, or with a fork rerun, so refresh it only when the fork run id or attempt changed.
    const recent = manual ? [] : prs.filter(pr => now - Date.parse(pr.updated_at) < RECENT_MS)
      .sort((a, b) => Date.parse(b.updated_at) - Date.parse(a.updated_at));
    // Probes need up to three calls, so rotate them too and keep a third of the budget for older PRs.
    // Later passes of the same sweep resume where the previous pass stopped.
    const recentBudget = Math.floor(budget * 2 / 3);
    const recentStart = !recent.length ? 0 : (recentCursor ??
      Math.floor(now / slotMs) * Math.max(1, Math.floor(recentBudget / 3))) % recent.length;
    let probed = 0;
    for (let offset = 0; offset < recent.length; offset++) {
      if (calls + 15 > recentBudget) break; // One PR may need trees, runs, paginated jobs and a status write.
      const pr = recent[(recentStart + offset) % recent.length];
      probed++;
      await visit(pr, async () => {
        const latest = await forkStatus(pr);
        if (latest && statusCompleted(latest) && pr.head.repo) {
          const shown = parseRunUrl(latest.target_url);
          // A fork without fork-ci.yml (404) has no run to watch; its status already says so.
          const fork = (await service.findRun(pr, false).catch(error => {
            if (error.status !== 404) throw error;
          }))?.run;
          // Fork reruns, Run workflow, trusted reruns and new dispatches start without a PR-head
          // signal. The result is settled only while every run it depends on is unchanged.
          const trusted = (await service.findRun(pr, true))?.run;
          const same = (a, id, attempt) => a?.id === id && a.run_attempt === attempt && a.status === 'completed';
          const settled = !shown ? !fork && !trusted
            : shown.trusted ? same(trusted, shown.run_id, shown.run_attempt) &&
              (fork ? same(fork, shown.fork_run_id, shown.fork_run_attempt) : !shown.fork_run_id)
              : same(fork, shown.run_id, shown.run_attempt) && !trusted;
          if (settled) return;
        }
        await refresh(pr);
      });
    }
    recentCursor = recentStart + probed;
    // Rotate the rest using trusted wall-clock time, so old PRs are eventually visited without
    // accepting a contributor-writable cursor. Advance by about one sweep's capacity.
    const step = Math.max(1, Math.floor(budget / 20));
    const start = manual || !prs.length ? 0 : (Math.floor(now / slotMs) * step) % prs.length;
    for (let offset = 0; offset < prs.length; offset++) {
      if (calls + 15 > budget) break;
      const pr = prs[(start + offset) % prs.length];
      if (visited.has(pr.number)) continue;
      await visit(pr, () => refresh(pr));
    }
    core.info(`Visited ${processed}/${prs.length} PRs (probed ${probed}/${recent.length} recently updated); ` +
      `${calls}/${budget} API calls; starting index ${start}.`);
  }
  // Start passes on a fixed PASS_MS cadence: the time a pass spends on API calls does not
  // delay the next one.
  const started = clock();
  for (let i = 0; i < passes; i++) {
    if (i) {
      const next = Math.max(clock(), started + i * PASS_MS);
      if (next >= started + LOOP_MS) break;
      await sleep(next - clock());
      calls = 0;
    }
    await pass(Math.floor(budget / passes));
  }
  if (manual && processed < prs.length) core.setFailed('API budget exhausted; dispatch again for a specific PR after the quota resets.');
  if (failures.size) core.setFailed(`Unable to refresh: ${[...failures].map(n => `#${n}`).join(', ')}`);
}
module.exports = { update };
