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

const RECENT_MS = 6 * 60 * 60 * 1000;

async function update({ github, context, core }) {
  const signal = context.eventName === 'workflow_run' && context.payload.workflow_run.name === 'Fork CI PR Signal';
  const trustedSignal = context.eventName === 'workflow_run' &&
    context.payload.workflow_run.event === 'workflow_dispatch' &&
    /^Trusted Fork CI #([1-9][0-9]*)$/.exec(context.payload.workflow_run.display_title || '');
  const manual = context.eventName === 'workflow_dispatch' || !!trustedSignal || signal;
  const requested = context.payload.inputs?.['pull-request'] || (trustedSignal && trustedSignal[1]);
  if (requested && !/^[1-9][0-9]*$/.test(requested)) throw new Error('Invalid PR number.');
  let prs = requested
    ? [(await github.rest.pulls.get({ ...context.repo, pull_number: Number(requested) })).data]
    : await github.paginate(github.rest.pulls.list, { ...context.repo, state: 'open', per_page: 100 });
  if (signal) prs = prs.filter(pr => pr.head.sha === context.payload.workflow_run.head_sha);
  const now = Date.now();
  // Never restore decisions or scheduling state from Actions caches: PR code dispatched
  // on the default branch can write those caches using its runner runtime token.
  const service = createService(github);
  const availableBases = new Map();
  const rate = (await github.rest.rateLimit.get()).data.resources.core;
  // Bound work even while settled successes are watched. Rotate the starting PR to avoid starving
  // older PRs, and leave quota for signal workflows and other repository automation. Organizations on
  // GitHub Enterprise Cloud, such as apache, get 15,000 requests per hour: spend at most about half of
  // the hourly quota across five-minute sweeps, and never less than 60 calls per sweep.
  const perSweep = Math.max(60, Math.floor(rate.limit / 24));
  const budget = manual ? Math.max(0, rate.remaining - 100) : Math.min(perSweep, Math.max(0, rate.remaining - 100));
  let calls = 0;
  github.hook.before('request', () => { calls++; });
  const failures = [];
  const recordOf = check => { try { return JSON.parse(check?.output?.text || '{}'); } catch { return {}; } };
  async function latestCheck(pr) {
    const checks = await github.paginate(github.rest.checks.listForRef, {
      ...context.repo, ref: pr.head.sha, check_name: 'Fork CI', per_page: 100, filter: 'all'
    });
    return checks.filter(check => check.app?.slug === 'github-actions').sort((a, b) => b.id - a.id)[0];
  }
  async function refresh(pr) {
    // Release branches without this CI migration keep their own pull_request workflows.
    if (!availableBases.has(pr.base.sha)) {
      availableBases.set(pr.base.sha, github.rest.repos.getContent({ ...context.repo,
        path: '.github/workflows/fork-ci.yml', ref: pr.base.sha }).then(() => true, error => {
        if (error.status === 404) return false;
        throw error;
      }));
    }
    if (!(await availableBases.get(pr.base.sha))) return;
    const mirror = await service.mirror(pr);
    const { result, run } = mirror;
    const note = result.note || (result.status !== 'completed' ? 'Waiting for the fork run.'
      : result.conclusion === 'success' ? 'Verified all caller suites.' : 'Open the run to see which jobs did not succeed.');
    const testedBase = mirror.base && mirror.base !== pr.base.sha ? `\nTested base: ${mirror.base}` : '';
    const record = { owner: mirror.owner, repo: mirror.repo, run_id: run?.id,
      run_attempt: run?.run_attempt, base_ref: pr.base.ref, base_sha: pr.base.sha, trusted: mirror.trusted || false,
      fork_run_id: mirror.fork?.id, fork_run_attempt: mirror.fork?.run_attempt };
    const details = run ? `https://github.com/${mirror.owner}/${mirror.repo}/actions/runs/${run.id}` : pr.html_url;
    const output = { title: result.title,
      // GitHub ignores details_url for checks created with GITHUB_TOKEN, so link the run here.
      summary: (run ? `[See fork CI results](${details})\n\n` : '') + note +
        `\n\nBase: ${pr.base.ref} @ ${pr.base.sha}${testedBase}`,
      text: JSON.stringify(record) };
    // Re-read before publishing so a push or retarget during the API calls cannot get a stale success.
    const { data: current } = await github.rest.pulls.get({ ...context.repo, pull_number: pr.number });
    if (current.head.sha !== pr.head.sha || current.base.sha !== pr.base.sha || current.base.ref !== pr.base.ref || current.state !== 'open') return;
    const params = { ...context.repo, ...result, output, details_url: details };
    delete params.title;
    delete params.note;
    // Signals and sweeps can publish concurrently. Read the check last and never replace a newer
    // attempt of a run from the same repository, or a completed result with a running one.
    const latest = await latestCheck(pr);
    const oldRecord = recordOf(latest);
    if (oldRecord.owner === record.owner && oldRecord.repo === record.repo && record.run_id &&
        (oldRecord.run_id > record.run_id || (oldRecord.run_id === record.run_id &&
          (oldRecord.run_attempt > record.run_attempt ||
            (oldRecord.run_attempt === record.run_attempt && latest.status === 'completed' && result.status !== 'completed'))))) return;
    const sameRun = oldRecord.run_id === record.run_id && oldRecord.run_attempt === record.run_attempt && oldRecord.base_sha === record.base_sha && oldRecord.base_ref === record.base_ref;
    if (!latest || (latest.status === 'completed' && (!sameRun || result.status !== 'completed'))) {
      await github.rest.checks.create({ ...params, name: 'Fork CI', head_sha: pr.head.sha });
    } else if (!sameRun || latest.status !== result.status || latest.conclusion !== (result.conclusion || null) || latest.output?.summary !== output.summary || latest.output?.text !== output.text) {
      await github.rest.checks.update({ ...params, check_run_id: latest.id });
    }
  }
  async function visit(pr, work) {
    processed++;
    visited.add(pr.number);
    if (pr.state !== 'open') return;
    try {
      await work();
    } catch (error) {
      failures.push(pr.number);
      core.warning(`Could not refresh #${pr.number}: ${error.message}`);
    }
  }
  let processed = 0;
  const visited = new Set();
  // Fork runs, including reruns, finish without an upstream event. Such PRs were updated recently,
  // so probe those first. A completed result changes only with a push or a trusted run, which both
  // signal a refresh, or with a fork rerun, so refresh it only when the fork run id or attempt changed.
  const recent = manual ? [] : prs.filter(pr => now - Date.parse(pr.updated_at) < RECENT_MS)
    .sort((a, b) => Date.parse(b.updated_at) - Date.parse(a.updated_at));
  // Probes take two calls, so rotate them too and keep a third of the budget for older PRs.
  const recentBudget = Math.floor(budget * 2 / 3);
  const recentStart = !recent.length ? 0 : (Math.floor(now / 300000) * Math.max(1, Math.floor(recentBudget / 4))) % recent.length;
  let probed = 0;
  for (let offset = 0; offset < recent.length; offset++) {
    if (calls + 15 > recentBudget) break; // One PR may need trees, runs, paginated jobs and a check write.
    const pr = recent[(recentStart + offset) % recent.length];
    probed++;
    await visit(pr, async () => {
      const latest = await latestCheck(pr);
      if (latest?.status === 'completed') {
        if (!pr.head.repo) return;
        const record = recordOf(latest);
        const fork = (await service.findRun(pr, false))?.run;
        if (!fork || (fork.id === (record.fork_run_id ?? record.run_id) &&
            fork.run_attempt === (record.fork_run_attempt ?? record.run_attempt))) return;
      }
      await refresh(pr);
    });
  }
  // Rotate the rest using trusted wall-clock time, so old PRs are eventually visited without
  // accepting a contributor-writable cursor. Advance by about one sweep's capacity.
  const step = Math.max(1, Math.floor(budget / 20));
  const start = manual || !prs.length ? 0 : (Math.floor(now / 300000) * step) % prs.length;
  for (let offset = 0; offset < prs.length; offset++) {
    if (calls + 15 > budget) break;
    const pr = prs[(start + offset) % prs.length];
    if (visited.has(pr.number)) continue;
    await visit(pr, () => refresh(pr));
  }
  core.info(`Visited ${processed}/${prs.length} PRs (probed ${probed}/${recent.length} recently updated); ` +
    `${calls}/${budget} API calls; starting index ${start}.`);
  if (manual && processed < prs.length) core.setFailed('API budget exhausted; dispatch again for a specific PR after the quota resets.');
  if (failures.length) core.setFailed(`Unable to refresh: ${failures.map(n => `#${n}`).join(', ')}`);
}
module.exports = { update };
