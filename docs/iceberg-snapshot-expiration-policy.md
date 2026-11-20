---
title: "Iceberg Snapshot Expiration Policy"
slug: "/iceberg-snapshot-expiration-policy"
keyword: "iceberg, snapshot, expiration, policy, optimizer, Gravitino"
license: "This software is licensed under the Apache License version 2."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Overview

`system_iceberg_snapshot_expiration` is a built-in policy type used by the optimizer and TMS to
expire old Iceberg snapshots through `builtin-iceberg-expire-snapshots`.

This policy supports `CATALOG`, `SCHEMA`, and `TABLE` metadata objects.

## Policy Content

The typed content for `system_iceberg_snapshot_expiration` supports the following fields:

| Field | Required | Default | Description |
| --- | --- | --- | --- |
| `olderThanDays` | No | `5` | Minimum age of snapshots eligible for expiration, in days. Must be between `1` and `36500`. |
| `retainLast` | No | `1` | Minimum number of recent snapshots to keep regardless of age. Must be `>= 1`. |
| `streamResults` | No | `false` | Whether the expire job should stream intermediate delete results. JSON boolean only. |

## Generated Rules and Properties

The policy content is converted to:

- Properties:
  - `strategy.type=iceberg-snapshot-expiration`
  - `job.template-name=builtin-iceberg-expire-snapshots`
- Rules:
  - `trigger-expr=true`
  - `score-expr=1`
  - `job.options.olderThanDays=<olderThanDays>`
  - `job.options.retainLast=<retainLast>`
  - `job.options.streamResults=<streamResults>`

Eligibility for snapshot expiry is evaluated when the optimizer or TMS schedule runs; the
in-Spark gate always maintains once a job starts.

## TMS before and after metrics

`builtin-iceberg-maintenance-task` reports these Validation metrics:

| Phase | Key | Source |
| --- | --- | --- |
| Before (S0) | `custom-snapshot-number` | Snapshot count before expire |
| After (S1) | `custom-snapshot-number` | Measured snapshot count after expire (not before minus expired) |
| After (S1) | `custom-expired-snapshot-number` | Snapshots present in S0 but absent after expire |
| After (S1) | `custom-expired-file-size` | Content-file bytes present in S0 but absent after expire |

## Policy Examples

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "iceberg_snapshot_expiration_default",
    "comment": "Built-in iceberg snapshot expiration policy",
    "policyType": "system_iceberg_snapshot_expiration",
    "enabled": true,
    "content": {}
  }' \
  http://localhost:8090/api/metalakes/test/policies
```

</TabItem>
<TabItem value="java" label="Java">

```java
GravitinoClient client = ...;

PolicyContent content = PolicyContents.icebergSnapshotExpiration();

Policy policy =
    client.createPolicy(
        "iceberg_snapshot_expiration_default",
        "system_iceberg_snapshot_expiration",
        "Built-in iceberg snapshot expiration policy",
        true,
        content);
```

</TabItem>
</Tabs>

## Apply the Policy Through a Tag

After creating the policy, create or reuse a tag, associate the policy with that tag using the
`ALL_VALUES` selector, and assign the tag to a table or one of its ancestors. See
[Manage Policies](./manage-policies-in-gravitino.md#policy-to-tag-associations) and
[Snapshot expiration](./table-maintenance-service/optimizer-configuration.md#snapshot-expiration).
