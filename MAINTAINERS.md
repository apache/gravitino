<!--
  Copyright 2023 Datastrato Pvt Ltd.
  This software is licensed under the Apache License version 2.
-->

This document lists the maintainers and contributors of the Project.

# Maintainers

Maintainers may be added once approved by the existing maintainers (see [Governance document](GOVERNANCE.md)). By adding your name to this list you agree to follow the project's guidelines and governance including the [Code of Conduct](CODE-OF-CONDUCT.md) and that you have permission to contribute from your employer. All contributions are licensed under the Apache License version 2.

| **NAME**      | **GitHub Username** | **Organization** |
|---------------|---------------------|------------------|
| Justin Mclean | justinmclean        | Datastrato       |
| Heng Qin      | qqqttt123           | Datastrato       |
| McHades       | mchades             | Datastrato       |
| Xun Liu       | xunliu              | Datastrato       |
| Hui Yu        | diqiu50             | Datastrato       |
| Xiaojing Fang | FANNG1              | Datastrato       |
| Qi Yu         | yuqi1129            | Datastrato       |
| Clearvive     | Clearvive           | Datastrato       |
| Cheyne        | ch3yne              | Datastrato       |
| Jerry Shao    | jerryshao           | Datastrato       |
| Shaofeng Shi  | shaofengshi         | Datastrato       |
| Lisa Cao      | lisancao            | Datastrato       |
| Qian Xia      | LauraXia123         | Datastrato       |
| Danhua Wang   | danhuawang          | Datastrato       |

# Contributors

Contributors may be added by existing maintainers (see [Governance document](GOVERNANCE.md)). By adding your name to this list you agree to follow the project's guidelines and governance including the [Code of Conduct](CODE-OF-CONDUCT.md) and that you have permission to contribute from your employer. All contributions are licensed under the Apache License version 2.

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
   - Starting from 0.6.0, we will use the "labels" to manage the release versions, so please add
     the corresponding labels to the issue. For example, if the issue is fixed in 0.6.0, please
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
