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

// Jobs of one run can only be re-run after the whole run completes, and the slowest suite takes
// hours. Cancel the run as soon as any job fails, so the author can re-run it right away.
async function watch({ github, context, core, self, interval = 60000, sleep = ms => new Promise(resolve => setTimeout(resolve, ms)) }) {
  const attempt = Number(process.env.GITHUB_RUN_ATTEMPT || 1);
  let idle = 0;
  // Dependent jobs appear only once their needs finish, so require two quiet polls before exiting.
  while (idle < 2) {
    const jobs = (await github.paginate(github.rest.actions.listJobsForWorkflowRunAttempt, {
      ...context.repo, run_id: context.runId, attempt_number: attempt, per_page: 100
    })).filter(job => job.name !== self);
    const failed = jobs.filter(job => STOPPED.includes(job.conclusion)).map(job => job.name);
    const running = jobs.filter(job => job.status !== 'completed').map(job => job.name);
    core.info(`${jobs.length - running.length}/${jobs.length} jobs completed, ` +
      `${failed.length} failed` + (running.length ? `; waiting for ${running.slice(0, 5).join(', ')}` +
      (running.length > 5 ? ` and ${running.length - 5} more` : '') : ''));
    if (failed.length) {
      core.error(`Cancelling the run because these jobs failed: ${failed.join(', ')}. Re-run failed jobs once it is cancelled.`);
      await github.rest.actions.cancelWorkflowRun({ ...context.repo, run_id: context.runId });
      return failed;
    }
    idle = running.length ? 0 : idle + 1;
    if (idle < 2) await sleep(interval);
  }
  core.info('All jobs completed without a failure.');
  return [];
}
module.exports = { watch };
