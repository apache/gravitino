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

// Custom checks created with GITHUB_TOKEN can be attached to an older Actions suite and
// disappear from the PR's latest-check view. Publish a commit status as the visible signal;
// retain the check run separately for the immutable run/attempt metadata used by the updater.
async function publishStatus({ github, repo, sha, context, state, description, targetUrl }) {
  const params = { ...repo, sha, context, state, description: description.slice(0, 140), target_url: targetUrl };
  const { data } = await github.rest.repos.getCombinedStatusForRef({ ...repo, ref: sha, per_page: 100 });
  const previous = data.statuses.find(status => status.context === context);
  if (previous?.state === state && previous.description === params.description && previous.target_url === targetUrl) return;
  await github.rest.repos.createCommitStatus(params);
}

// Waiting for a committer is not a failure of the contributor's change.
const TRUSTED_REQUIRED = 'Fork CI passed; trusted verification required';

// Accepts a computed result ({ status, conclusion, title }) or a check run (title in output).
function statusState(result) {
  if (result.status !== 'completed') return 'pending';
  if ((result.title ?? result.output?.title) === TRUSTED_REQUIRED) return 'pending';
  if (result.conclusion === 'success') return 'success';
  return result.conclusion === 'failure' || result.conclusion === 'timed_out' ? 'failure' : 'error';
}

module.exports = { TRUSTED_REQUIRED, publishStatus, statusState };
