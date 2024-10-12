<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

Note: This maintainer and contributor tables at the end of this document list the maintainers and contributors of the project before it become an ASF project and are no longer updated.

# Committers

The current Apache Gravitino project committers can be [viewed here](https://projects.apache.org/project.html?incubator-gravitino).

## Review process

All contributions to the project must be reviewed by **AT LEAST** one maintainer before merging.
For larger changes, it is recommended to have more than one reviewer. In particular, if you are
working on an area you are not familiar with, it is recommended to ask for review from that area by
using `git log --format=full <filename>` to see who committed the most.

## When to commit/merge a pull request

PRs shall not be merged during active, ongoing discussions, unless they address issues such as
critical security fixes or public vulnerabilities. Time should be given prior to merging for
those involved with conversations to be resolved.

## How to merge a pull request

Changes pushed to the main branch on Gravitino cannot be **removed**, that is, we can't do
force-push to it. So please don't add any test commits or anything like that. If the PR is
merged by mistake, please using `git revert` to revert the commit, not `git reset` or anything
like that.

Please use the "Squash and merge" option when merging a PR. This will keep the commit history
clean and meaningful.

Gravitino uses "issue" to track the project progresses. So make sure each PR has a related issue, if
not, please ask the PR author to create one. Unless this PR is a trivial one, like fixing a typo or
something like that, all PRs should have related issues.

1. Before merging, make sure the PR is approved by **at least** one maintainer.
2. Check and modify the PR title and description if necessary to follow the project's guidelines.
3. Make sure the PR has a related issue, if not, please ask the PR author to create one; if wrong,
   please correct the PR title and description.
4. Assign the "Assignees" to the PR author.
5. If this PR needs to be backported to other branches, please add the corresponding labels to the
   PR, like "branch-0.5", "branch-0.6", etc. GitHub Actions will automatically create a backport
   PR for you.
6. After PR is merged, please check the related issue:
   - If the issue is not closed, please close it as fixed manually.
   - Assign the issue "Assignees" to the PR author.
   - Starting from 0.6.0-incubating, we will use the "labels" to manage the release versions, so please add
     the corresponding labels to the issue. For example, if the issue is fixed in 0.6.0-incubating, please
     add the label "0.6.0". If the issue is fixed both in 0.6.0 and 0.5.1, please add both labels.

## Policy on backporting bug fixes

Gravitino maintains several branches for different versions, so backporting bug fixes is a
common and necessary operation for maintenance releases. The decision point is when you have a
bug fix, and it's not clear whether it is worth backporting or not. Here is the general policy:

For those bug fixes we should backport:

1. Both the bug and fix are well understood and isolated, that is, the fix is not a large change
   that could introduce new bugs. Code being modified is well tested.
2. The bug being addressed is high priority or critical to the community.
3. The backported fix does not vary widely from the main branch fix.

For those bug fixes we should not backport:

1. The bug or fix is not well understood. The code is not well tested. The fix is a large change
   that could introduce new bugs.
2. The bug being addressed is low priority or not critical to the community.
3. The backported fix varies widely from the main branch fix.

# Pre-ASF Maintainers

Maintainers of the project are now called committers.

All contributions from the people listed are licensed under the Apache License version 2.

| **NAME**      | **GitHub Username** | **Organization** |
|---------------|---------------------|------------------|
| Justin Mclean | justinmclean        | Datastrato       |
| He Qi         | qqqttt123           | Datastrato       |
| Minghuang Li  | mchades             | Datastrato       |
| Xun Liu       | xunliu              | Datastrato       |
| Hui Yu        | diqiu50             | Datastrato       |
| Xiaojing Fang | FANNG1              | Datastrato       |
| Qi Yu         | yuqi1129            | Datastrato       |
| Decheng Liu   | ch3yne              | Datastrato       |
| Saisai Shao   | jerryshao           | Datastrato       |
| Shaofeng Shi  | shaofengshi         | Datastrato       |
| Lisa Cao      | lisancao            | Datastrato       |
| Qian Xia      | LauraXia123         | Datastrato       |
| Danhua Wang   | danhuawang          | Datastrato       |

# Pre-ASF Contributors

All contributions from the people listed are licensed under the Apache License version 2.

| **NAME**       | **GitHub Username** | **Organization** |
|----------------|---------------------|------------------|
| Kuan-Po Tseng  | brandboat           | SUSE             |
| Nicholas Jiang | SteNicholas         | Bilibili         |
| Eric Chang     | unknowntpo          | Lawsnote         |
| Sophie Sun     | SophieTech88        | ExtraHop Network |
| Xing Yong      | YxAc                | Xiaomi           |
| Liwei Yang     | lw-yang             | Xiaomi           |
| Yu-Ting Wang   | noidname01          | opensource4you   |
| Ziva Li        | zivali              | Yahoo            |
| Kang Zhou      | zhoukangcn          | Xiaomi           |
| Han Zhang      | xiaozcy             | Xiaomi           |
| Yu-Hsin Lai    | laiyousin           | Virginia Tech    |
| Charlie Cheng  | charliecheng630     | cacaFly          |
| PoAn Yang      | FrankYang0592       | SUSE             |
| Congling Xia   | xiacongling         | Xiaomi           |
| JieBao Xiao    | xloya               | Xiaomi           |
| Can Cai        | caican00            | Xiaomi           |
| Peidian Li     | coolderli           | Xiaomi           |
| Brandon Lu     | Lanznx              | LINE             |
| Lewis Jackson  | xnge                | opensource4you   |
| Li-Hsing Liu   | austin362667        | opensource4you   |
| Tianhang Li    | TEOTEO520           | Bilibili         |
| Hiren Sharma   | hiirrxnn            | opensource4you   |
| Chun-Hung Tseng| henrybear327        | opensource4you   |
| Carl Chang     | ichuniq             | opensource4you   |
