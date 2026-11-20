---
title: "Iceberg Orphan File Removal Policy"
slug: "/iceberg-orphan-file-removal-policy"
keyword: "iceberg, orphan, cleanup, policy, optimizer, Gravitino"
license: "This software is licensed under the Apache License version 2."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Overview

`system_iceberg_orphan_file_removal` is a built-in policy type used by the optimizer and TMS to
remove unreferenced Iceberg files through `builtin-iceberg-remove-orphan-files`.

This policy supports `CATALOG`, `SCHEMA`, and `TABLE` metadata objects. It accepts time triggers
only; `commitTrigger=true` is rejected.

## Policy Content

The typed content for `system_iceberg_orphan_file_removal` supports the following fields:

| Field | Required | Default | Description |
| --- | --- | --- | --- |
| `olderThanDays` | No | `3` | Minimum age of orphan files eligible for removal, in days. Must be between `1` and `36500`. |
| `location` | No | Table location | Optional absolute scan path within the table. Must be a valid absolute URI path without query, fragment, `..`, `\`, or percent-encoding. |
| `dryRun` | No | `false` | When `true`, list candidates without deleting. JSON boolean only. |

## Generated Rules and Properties

The policy content is converted to:

- Properties:
  - `strategy.type=iceberg-orphan-file-removal`
  - `job.template-name=builtin-iceberg-remove-orphan-files`
- Rules:
  - `trigger-expr=true`
  - `score-expr=1`
  - `job.options.olderThanDays=<olderThanDays>`
  - `job.options.dryRun=<dryRun>`
  - `job.options.location=<location>` when `location` is set

Eligibility for orphan cleanup is evaluated when the optimizer or TMS schedule runs; the
in-Spark gate always maintains once a job starts.

## TMS before and after metrics

`builtin-iceberg-maintenance-task` reports these Validation metrics:

| Phase | Key | Source |
| --- | --- | --- |
| Before (S0) | _(none)_ | Orphan cleanup skips before-metrics |
| After (S1) | `custom-removed-file-number` | Orphan files removed, or found when `dryRun` is true |
| After (S1) | `custom-removed-file-size` | Bytes of orphan files removed (or found in dry-run) |

## Policy Examples

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "iceberg_orphan_cleanup_default",
    "comment": "Built-in iceberg orphan file removal policy",
    "policyType": "system_iceberg_orphan_file_removal",
    "enabled": true,
    "content": {}
  }' \
  http://localhost:8090/api/metalakes/test/policies
```

</TabItem>
<TabItem value="java" label="Java">

```java
GravitinoClient client = ...;

PolicyContent content = PolicyContents.icebergOrphanFileRemoval();

Policy policy =
    client.createPolicy(
        "iceberg_orphan_cleanup_default",
        "system_iceberg_orphan_file_removal",
        "Built-in iceberg orphan file removal policy",
        true,
        content);
```

</TabItem>
</Tabs>

## Apply the Policy Through a Tag

After creating the policy, create or reuse a tag, associate the policy with that tag using the
`ALL_VALUES` selector, and assign the tag to a table or one of its ancestors. See
[Manage Policies](./manage-policies-in-gravitino.md#policy-to-tag-associations) and
[Remove Orphan Files](./table-maintenance-service/optimizer-cli-reference.md#remove-orphan-files).
