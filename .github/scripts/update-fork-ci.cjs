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
const fs = require('node:fs');
const { createService, skippable } = require('./fork-ci-status.cjs');

async function update({ github, context, core, stateFile = '.fork-ci-status/state.json' }) {
  const signal = context.eventName === 'workflow_run' && context.payload.workflow_run.name === 'Fork CI PR Signal';
  const trustedSignal = context.eventName === 'workflow_run' &&
    context.payload.workflow_run.event === 'workflow_dispatch' &&
    /^Trusted Fork CI #([1-9][0-9]*)$/.exec(context.payload.workflow_run.display_title || '');
  const manual = context.eventName === 'workflow_dispatch' || !!trustedSignal || signal;
  const requested = context.payload.inputs?.['pull-request'] || (trustedSignal && trustedSignal[1]);
  if (requested && !/^[1-9][0-9]*$/.test(requested)) throw new Error('Invalid PR number.');
  let previous = { prs: {}, cursor: 0 };
  try { previous = JSON.parse(fs.readFileSync(stateFile, 'utf8')); } catch { /* Cold cache. */ }
  if (!previous.prs || !Number.isInteger(previous.cursor)) previous = { prs: {}, cursor: 0 };
  let prs = requested
    ? [(await github.rest.pulls.get({ ...context.repo, pull_number: Number(requested) })).data]
    : await github.paginate(github.rest.pulls.list, { ...context.repo, state: 'open', per_page: 100 });
  if (signal) prs = prs.filter(pr => pr.head.sha === context.payload.workflow_run.head_sha);
  const now = Date.now();
  const definitions = new Map();
  for (const state of Object.values(previous.prs)) {
    if (typeof state.definitions_equal === 'boolean' && state.repository) {
      definitions.set(`${state.repository}@${state.sha}:${state.base}`, state.definitions_equal);
    }
  }
  const service = createService(github, definitions);
  const next = { prs: { ...previous.prs }, cursor: previous.cursor };
  const availableBases = new Map();
  const rate = (await github.rest.rateLimit.get()).data.resources.core;
  // Bound work even while settled successes are watched. Preserve a cursor to avoid starving
  // older PRs, and leave quota for signal workflows and other repository automation.
  const budget = manual ? Math.max(0, rate.remaining - 100) : Math.min(60, Math.max(0, rate.remaining - 100));
  let calls = 0;
  github.hook.before('request', () => { calls++; });
  const failures = [];
  const start = manual || !prs.length ? 0 : previous.cursor % prs.length;
  let processed = 0;
  for (let offset = 0; offset < prs.length; offset++) {
    if (calls + 15 > budget) break; // One PR may need trees, runs, paginated jobs and a check write.
    const index = (start + offset) % prs.length;
    const pr = prs[index];
    processed++;
    if (!manual) next.cursor = (index + 1) % prs.length;
    if (pr.state !== 'open') continue;
    if (skippable(pr, previous.prs[pr.number], now, manual)) continue;
    try {
      // Release branches without this CI migration keep their own pull_request workflows.
      if (!availableBases.has(pr.base.sha)) {
        availableBases.set(pr.base.sha, github.rest.repos.getContent({ ...context.repo,
          path: '.github/workflows/fork-ci.yml', ref: pr.base.sha }).then(() => true, error => {
          if (error.status === 404) return false;
          throw error;
        }));
      }
      if (!(await availableBases.get(pr.base.sha))) continue;
      const mirror = await service.mirror(pr);
      const { result, run } = mirror;
      const checks = await github.paginate(github.rest.checks.listForRef, {
        ...context.repo, ref: pr.head.sha, check_name: 'Fork CI', per_page: 100, filter: 'all'
      });
      const latest = checks.filter(check => check.app?.slug === 'github-actions').sort((a, b) => b.id - a.id)[0];
      const record = { owner: mirror.owner, repo: mirror.repo, run_id: run?.id,
        run_attempt: run?.run_attempt, base_ref: pr.base.ref, base_sha: pr.base.sha, trusted: mirror.trusted || false };
      const details = run ? `https://github.com/${mirror.owner}/${mirror.repo}/actions/runs/${run.id}` : pr.html_url;
      const output = { title: result.title,
        summary: `${result.note || 'Verified all caller suites.'}\n\nBase: ${pr.base.ref} @ ${pr.base.sha}`,
        text: JSON.stringify(record) };
      // Re-read before publishing so a push or retarget during the API calls cannot get a stale success.
      const { data: current } = await github.rest.pulls.get({ ...context.repo, pull_number: pr.number });
      if (current.head.sha !== pr.head.sha || current.base.sha !== pr.base.sha || current.base.ref !== pr.base.ref || current.state !== 'open') continue;
      const params = { ...context.repo, ...result, output, details_url: details };
      delete params.title;
      delete params.note;
      let oldRecord;
      try { oldRecord = JSON.parse(latest?.output?.text || '{}'); } catch { oldRecord = {}; }
      const sameRun = oldRecord.run_id === record.run_id && oldRecord.run_attempt === record.run_attempt && oldRecord.base_sha === record.base_sha && oldRecord.base_ref === record.base_ref;
      if (!latest || (latest.status === 'completed' && (!sameRun || result.status !== 'completed'))) {
        await github.rest.checks.create({ ...params, name: 'Fork CI', head_sha: pr.head.sha });
      } else if (!sameRun || latest.status !== result.status || latest.conclusion !== (result.conclusion || null) || latest.output?.summary !== output.summary) {
        await github.rest.checks.update({ ...params, check_run_id: latest.id });
      }
      next.prs[pr.number] = { sha: pr.head.sha, base: pr.base.sha, base_ref: pr.base.ref,
        repository: pr.head.repo?.full_name,
        definitions_equal: pr.head.repo ? definitions.get(`${pr.head.repo.full_name}@${pr.head.sha}:${pr.base.sha}`) : undefined,
        updated_at: pr.updated_at, status: result.status, checked_at: new Date(now).toISOString(),
        completed_at: run?.updated_at || new Date(now).toISOString() };
    } catch (error) {
      delete next.prs[pr.number];
      failures.push(pr.number);
      core.warning(`Could not refresh #${pr.number}: ${error.message}`);
    }
  }
  fs.mkdirSync(require('node:path').dirname(stateFile), { recursive: true });
  fs.writeFileSync(stateFile, JSON.stringify(next));
  core.info(`Visited ${processed}/${prs.length} PRs; ${calls}/${budget} API calls; next cursor ${next.cursor}.`);
  if (manual && processed < prs.length) core.setFailed('API budget exhausted; dispatch again for a specific PR after the quota resets.');
  if (failures.length) core.setFailed(`Unable to refresh: ${failures.map(n => `#${n}`).join(', ')}`);
}
module.exports = { update };
