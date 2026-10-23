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
const UPSTREAM = 'apache/gravitino';

async function resolve({ github, context, core }) {
  const repository = `${context.repo.owner}/${context.repo.repo}`;
  let headRepository = repository;
  let headSha = context.sha;
  let baseRef = context.ref.replace(/^refs\/heads\//, '');
  let baseSha = headSha;
  let sync = false;
  // In a fork, "Run workflow" without a PR number starts a run for the selected branch, like a push.
  const trustedDispatch = context.eventName === 'workflow_dispatch' &&
    (repository === UPSTREAM || !!context.payload.inputs?.['pull-request']);
  if (trustedDispatch) {
    if (repository !== UPSTREAM) throw new Error('Trusted runs must be dispatched in apache/gravitino.');
    const number = context.payload.inputs?.['pull-request'];
    if (!/^[1-9][0-9]*$/.test(String(number))) throw new Error('A pull request number is required.');
    const { data: pr } = await github.rest.pulls.get({ ...context.repo, pull_number: Number(number) });
    if (pr.state !== 'open' || !pr.head.repo) throw new Error('The pull request must be open with an existing head repository.');
    baseRef = pr.base.ref;
    if (baseRef !== context.payload.repository.default_branch || context.ref !== `refs/heads/${baseRef}`) {
      throw new Error('Dispatch the trusted workflow from the current default branch; release branches use their own CI.');
    }
    // Pull requests retain an older base.sha after the base branch advances.
    const { data: base } = await github.rest.repos.getBranch({ ...context.repo, branch: baseRef });
    if (context.sha !== base.commit.sha) {
      throw new Error('Dispatch the trusted workflow from the current default branch; release branches use their own CI.');
    }
    headSha = pr.head.sha;
    headRepository = pr.head.repo.full_name;
    baseSha = base.commit.sha;
    sync = true;
  } else if (repository === UPSTREAM && (baseRef === 'main' || baseRef.startsWith('branch-'))) {
    // Post-merge CI tests the pushed upstream commit itself.
  } else {
    const { data: repo } = await github.rest.repos.get(context.repo);
    if (repo.fork && repo.source?.full_name === UPSTREAM) {
      const prs = await github.paginate(github.rest.pulls.list, {
        owner: 'apache', repo: 'gravitino', state: 'open',
        head: `${context.repo.owner}:${baseRef}`, per_page: 100
      });
      const matches = prs.filter(pr => pr.head.sha === headSha && pr.head.repo?.full_name === repository);
      if (matches.length > 1) throw new Error('More than one open PR matches this pushed commit.');
      if (matches.length === 1) {
        baseRef = matches[0].base.ref;
      } else {
        const { data: upstream } = await github.rest.repos.get({ owner: 'apache', repo: 'gravitino' });
        const branches = await github.paginate(github.rest.repos.listBranches, {
          owner: 'apache', repo: 'gravitino', per_page: 100
        });
        const releases = branches.map(b => b.name).filter(n => /^branch-\d+\.\d+$/.test(n))
          .sort((a, b) => a.localeCompare(b, 'en', { numeric: true })).slice(-3);
        const candidates = [...new Set([upstream.default_branch, ...releases])];
        let fewest = Infinity;
        for (const candidate of candidates) {
          const { data: comparison } = await github.rest.repos.compareCommitsWithBasehead({
            owner: 'apache', repo: 'gravitino',
            basehead: `${candidate}...${context.repo.owner}:${headSha}`, per_page: 1
          });
          if (comparison.ahead_by < fewest) { fewest = comparison.ahead_by; baseRef = candidate; }
        }
        core.notice(`No open PR: testing against ${baseRef}; results require a matching PR base.`);
      }
      const { data: base } = await github.rest.repos.getBranch({ owner: 'apache', repo: 'gravitino', branch: baseRef });
      baseSha = base.commit.sha;
      sync = true;
    } else if (repository === UPSTREAM) {
      // Same-repository feature branches may also belong to an open PR.
      const prs = await github.paginate(github.rest.pulls.list, {
        ...context.repo, state: 'open', head: `${context.repo.owner}:${baseRef}`, per_page: 100
      });
      const matches = prs.filter(pr => pr.head.sha === headSha && pr.head.repo?.full_name === repository);
      if (matches.length === 0) { core.setOutput('valid', 'false'); return; }
      if (matches.length !== 1) throw new Error('More than one PR matches this branch.');
      baseRef = matches[0].base.ref;
      const { data: base } = await github.rest.repos.getBranch({ ...context.repo, branch: baseRef });
      baseSha = base.commit.sha;
      sync = true;
    }
    // Standalone copies and unrelated fork networks test their own commit, without Apache sync.
  }
  if (!/^[0-9a-f]{40}$/.test(headSha) || !/^[0-9a-f]{40}$/.test(baseSha)) throw new Error('Invalid commit SHA.');
  if (!/^[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+$/.test(headRepository)) throw new Error('Invalid head repository.');
  for (const [name, value] of Object.entries({
    'base-ref': baseRef, 'base-sha': baseSha, 'head-sha': headSha,
    'head-repository': headRepository, sync: String(sync), valid: 'true'
  })) core.setOutput(name, value);
}
module.exports = { resolve };
