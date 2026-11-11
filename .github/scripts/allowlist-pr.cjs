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
const fs = require('node:fs/promises');
const path = require('node:path');
const { publishStatus } = require('./publish-ci-status.cjs');
const MAX_FILE = 1024 * 1024;
const MAX_TOTAL = 8 * MAX_FILE;

function yamlEntries(tree) {
  if (tree.truncated) throw new Error('Refusing a truncated .github tree.');
  return tree.tree.filter(entry => /\.ya?ml$/.test(entry.path)).map(entry => {
    if (!['100644', '100755'].includes(entry.mode) || entry.type !== 'blob' ||
        entry.path.split('/').some(part => part === '..' || part === '.' || !part) ||
        path.isAbsolute(entry.path) || entry.path.includes('\\') || entry.size > MAX_FILE) {
      throw new Error('Unsafe or oversized workflow data path.');
    }
    return entry;
  });
}
async function prepare({ github, context, core, fetchImpl = fetch, directory = path.join(process.env.RUNNER_TEMP, 'fork-ci-allowlist') }) {
  let prs;
  if (context.eventName === 'workflow_dispatch') {
    const number = context.payload.inputs?.['pull-request'];
    if (!/^[1-9][0-9]*$/.test(String(number))) throw new Error('A PR number is required.');
    prs = [(await github.rest.pulls.get({ ...context.repo, pull_number: Number(number) })).data];
  } else {
    const sha = context.payload.workflow_run.head_sha;
    if (!/^[0-9a-f]{40}$/.test(sha)) throw new Error('Invalid signal SHA.');
    // listPullRequestsAssociatedWithCommit does not return PRs whose head commit is in a fork.
    const run = context.payload.workflow_run;
    prs = (await github.paginate(github.rest.pulls.list, { ...context.repo, state: 'open',
      head: `${run.head_repository.owner.login}:${run.head_branch}`, per_page: 100 }))
      .filter(pr => pr.head.sha === sha);
  }
  prs = prs.filter(pr => pr.state === 'open' && pr.head.repo);
  if (!prs.length) { core.setOutput('prs', '[]'); return; }
  if (prs.some(pr => pr.head.sha !== prs[0].head.sha)) throw new Error('Inconsistent signal heads.');
  const pr = prs[0];
  const target = { owner: pr.head.repo.owner.login, repo: pr.head.repo.name };
  const { data: root } = await github.rest.git.getTree({ ...target, tree_sha: pr.head.sha });
  const entry = root.tree.find(e => e.path === '.github');
  if (!entry || entry.type !== 'tree') throw new Error('The PR has no .github directory.');
  const { data: tree } = await github.rest.git.getTree({ ...target, tree_sha: entry.sha, recursive: '1' });
  await fs.rm(directory, { recursive: true, force: true });
  let total = 0;
  for (const file of yamlEntries(tree)) {
    const url = `https://raw.githubusercontent.com/${target.owner}/${target.repo}/${pr.head.sha}/.github/` +
      file.path.split('/').map(encodeURIComponent).join('/');
    const response = await fetchImpl(url, { redirect: 'error', signal: AbortSignal.timeout(30000) });
    if (!response.ok) throw new Error(`Could not read workflow data: HTTP ${response.status}.`);
    const chunks = [];
    let size = 0;
    for await (const chunk of response.body) {
      size += chunk.length;
      total += chunk.length;
      if (size > MAX_FILE || total > MAX_TOTAL) throw new Error('Workflow data size limit exceeded.');
      chunks.push(chunk);
    }
    const destination = path.join(directory, '.github', file.path);
    await fs.mkdir(path.dirname(destination), { recursive: true });
    await fs.writeFile(destination, Buffer.concat(chunks));
  }
  // These are data files outside the trusted checkout. Never source, install or execute them.
  core.setOutput('prs', JSON.stringify(prs.map(pr => ({ number: pr.number, sha: pr.head.sha }))));
  core.setOutput('scan-glob', `${directory}/.github/**/*.y*ml`);
}
async function publish({ github, context, core }) {
  const prs = JSON.parse(process.env.SCANNED_PRS || '[]');
  for (const pr of prs) {
    const { data: current } = await github.rest.pulls.get({ ...context.repo, pull_number: pr.number });
    if (current.state !== 'open' || current.head.sha !== pr.sha) continue;
    const success = process.env.SCAN_OUTCOME === 'success';
    const details = `${context.serverUrl}/${context.repo.owner}/${context.repo.repo}/actions/runs/${context.runId}`;
    // Scanned PR YAML as data with the reviewed default-branch workflow; no PR code was executed.
    await publishStatus({ github, repo: context.repo, sha: pr.sha, context: 'ASF Allowlist Check',
      state: success ? 'success' : 'failure',
      description: success ? 'Workflow actions are allowlisted' : 'Workflow allowlist scan failed', targetUrl: details });
  }
  if (process.env.SCAN_OUTCOME !== 'success' && prs.length) core.setFailed('ASF allowlist scan failed.');
}
module.exports = { yamlEntries, prepare, publish };
