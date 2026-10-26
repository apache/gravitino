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
const STOPPED = ['failure', 'timed_out'];
const UPSTREAM = { owner: 'apache', repo: 'gravitino' };

// apache/gravitino cannot cancel runs in a contributor's fork, so a fork run stops itself once
// its pull request is closed. Returns the PR number when that happened.
function closedPullRequest({ github, context, core }) {
  const branch = context.ref.replace(/^refs\/heads\//, '');
  const inFork = context.repo.owner !== UPSTREAM.owner || context.repo.repo !== UPSTREAM.repo;
  let number;
  return async () => {
    if (!inFork || !context.ref.startsWith('refs/heads/')) return undefined;
    try {
      if (!number) {
        // The run can start before the PR is opened, so keep looking until it exists.
        const { data } = await github.rest.pulls.list({ ...UPSTREAM, state: 'open',
          head: `${context.repo.owner}:${branch}`, per_page: 10 });
        number = data.find(pr => pr.head.repo?.full_name === `${context.repo.owner}/${context.repo.repo}`)?.number;
        return undefined;
      }
      const { data } = await github.rest.pulls.get({ ...UPSTREAM, pull_number: number });
      return data.state === 'open' ? undefined : number;
    } catch (error) {
      core.warning(`Could not read the upstream pull request: ${error.message}.`);
      return undefined;
    }
  };
}

// Jobs of one run can only be re-run after the whole run completes, and the slowest suite takes
// hours. Cancel the run as soon as any job fails, so the author can re-run it right away.
async function watch({ github, context, core, self, interval = 60000, sleep = ms => new Promise(resolve => setTimeout(resolve, ms)) }) {
  const attempt = Number(process.env.GITHUB_RUN_ATTEMPT || 1);
  const closed = closedPullRequest({ github, context, core });
  let idle = 0;
  // Dependent jobs appear only once their needs finish, so require two quiet polls before exiting.
  while (idle < 2) {
    let jobs;
    try {
      jobs = (await github.paginate(github.rest.actions.listJobsForWorkflowRunAttempt, {
        ...context.repo, run_id: context.runId, attempt_number: attempt, per_page: 100
      })).filter(job => job.name !== self);
    } catch (error) {
      // The watcher only observes the suites. A transient API error must not fail a run whose
      // suites pass, so keep polling until the job times out.
      core.warning(`Could not list the run's jobs: ${error.message}. Retrying in a minute.`);
      await sleep(interval);
      continue;
    }
    const failed = jobs.filter(job => STOPPED.includes(job.conclusion)).map(job => job.name);
    const running = jobs.filter(job => job.status !== 'completed').map(job => job.name);
    core.info(`${jobs.length - running.length}/${jobs.length} jobs completed, ` +
      `${failed.length} failed` + (running.length ? `; waiting for ${running.slice(0, 5).join(', ')}` +
      (running.length > 5 ? ` and ${running.length - 5} more` : '') : ''));
    const closedPr = !failed.length && running.length ? await closed() : undefined;
    if (failed.length || closedPr) {
      if (failed.length) {
        core.error(`Cancelling the run because these jobs failed: ${failed.join(', ')}. Re-run failed jobs once it is cancelled.`);
      } else {
        core.notice(`Cancelling the run because apache/gravitino#${closedPr} was closed. ` +
          'After reopening it, re-run all jobs or start a new run with Run workflow.');
      }
      try {
        await github.rest.actions.cancelWorkflowRun({ ...context.repo, run_id: context.runId });
        return failed;
      } catch (error) {
        core.warning(`Could not cancel the run: ${error.message}. Retrying in a minute.`);
      }
    }
    idle = running.length ? 0 : idle + 1;
    if (idle < 2) await sleep(interval);
  }
  core.info('All jobs completed without a failure.');
  return [];
}
module.exports = { watch };
