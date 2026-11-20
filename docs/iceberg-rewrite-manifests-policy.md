---
title: "Iceberg Manifest Rewrite Policy"
slug: "/iceberg-rewrite-manifests-policy"
keyword: "iceberg, manifests, policy, optimizer, Gravitino"
license: "This software is licensed under the Apache License version 2."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Overview

`system_iceberg_rewrite_manifests` evaluates manifest statistics for one resolved partition
spec and submits `builtin-iceberg-rewrite-manifests`. The strategy type is
`iceberg-rewrite-manifests`; its handler and job adapter are built in.

This policy supports `CATALOG`, `SCHEMA`, and `TABLE` metadata objects.

## Policy Content

The typed content for `system_iceberg_rewrite_manifests` supports the following fields:

| Field                      | Required | Default                   | Description                                                                                                      |
| -------------------------- | -------- | ------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| `manifest_count_threshold` | No       | `100`                     | Trigger when manifest count is at least this value. Aligns with Iceberg `commit.manifest.min-count-to-merge`. Must be positive. |
| `spec_id`                  | No       | Iceberg's current default | Existing partition spec to evaluate. Omission resolves the current default from Iceberg metadata.                |
| `use_caching`              | No       | Omitted                   | Optional boolean forwarded to Iceberg; omission preserves the runtime default. JSON booleans only.               |

Spec IDs must be non-negative 32-bit integers. Recommendations are ranked by the selected
spec's manifest count. Average manifest size is collected for observability but does not
affect the trigger. At the default threshold, 99 manifests never trigger and 100 or more
trigger regardless of average manifest size.

## Generated Rules and Properties

The policy content is converted to:

- Properties:
  - `strategy.type=iceberg-rewrite-manifests`
  - `job.template-name=builtin-iceberg-rewrite-manifests`
- Rules:
  - `trigger-expr=custom-manifest-number >= manifest_count_threshold`
  - optional `spec_id` and `use_caching` job options when set

## Collect, evaluate, and submit

Submit `builtin-iceberg-update-manifest-stats` to collect complete
`custom-manifest-number-by-spec` and `custom-avg-manifest-size-by-spec` maps plus
`custom-manifest-snapshot-id` from one table snapshot. The collector covers every defined
spec in one traversal; it has no `spec_id` argument. Both maps are keyed by decimal spec ID.

The handler compares the published snapshot ID with `current-snapshot-id` in the table
metadata supplied by the optimizer's table metadata provider. Missing, malformed, or
mismatched snapshot IDs produce no recommendation; recollect before evaluating again.
For a table without a snapshot, the collector publishes `-1` and Iceberg table metadata
reports `none`. The provider must expose this metadata property to verify freshness.

The handler reads the chosen spec's count and average from the same statistics response.
A missing map or key produces no recommendation. An empty spec has count and average zero.
Invalid policy rules or malformed measurements are logged and skipped for that table without
aborting recommendations for other tables. Counts from other specs are ignored.

When `spec_id` is omitted, the recommender resolves Iceberg's current `default-spec-id` from
table metadata and retains that value through job submission. The maintenance job leaves an
omitted `spec_id` unset at schedule time and resolves the table's current default when the
Spark job runs. Set `spec_id` explicitly to target another existing spec after partition
evolution.

Rewriting reorganizes manifests within the selected spec and does not migrate data files
between specs. Recollect statistics after a successful rewrite before evaluating again.
For runtime requirements and direct job submission, see the
[rewrite-manifests job reference](./table-maintenance-service/optimizer-cli-reference.md).

## Policy Examples

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "rewrite_manifests",
    "comment": "Built-in iceberg rewrite manifests policy",
    "policyType": "system_iceberg_rewrite_manifests",
    "enabled": true,
    "content": {}
  }' \
  http://localhost:8090/api/metalakes/test/policies
```

</TabItem>
<TabItem value="java" label="Java">

```java
GravitinoClient client = ...;

PolicyContent content = PolicyContents.icebergRewriteManifests();

Policy policy =
    client.createPolicy(
        "rewrite_manifests",
        "system_iceberg_rewrite_manifests",
        "Built-in iceberg rewrite manifests policy",
        true,
        content);
```

</TabItem>
</Tabs>

## Apply the Policy Through a Tag

After creating the policy, create or reuse a tag, associate the policy with that tag using the
`ALL_VALUES` selector, and assign the tag to a table or one of its ancestors. See
[Manage Policies](./manage-policies-in-gravitino.md#policy-to-tag-associations) and
[Optimizer configuration](./table-maintenance-service/optimizer-configuration.md).
