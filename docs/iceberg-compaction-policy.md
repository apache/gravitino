---
title: "Iceberg compaction policy"
slug: /iceberg-compaction-policy
date: 2026-03-05
keyword: iceberg, compaction, policy, optimizer, Gravitino
license: This software is licensed under the Apache License version 2.
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Overview

`ICEBERG_COMPACTION` is a built-in policy type used by the optimizer to generate compaction strategies and job contexts for Iceberg tables.

This policy supports `CATALOG`, `SCHEMA`, and `TABLE` metadata objects.

## Policy content

The typed content for `ICEBERG_COMPACTION` supports the following fields:

| Field | Required | Default | Description |
|---|---|---|---|
| `minDataFileMse` | No | `405323966463344` | Minimum threshold for metric `custom-data-file-mse`. Must be `>= 0`. |
| `minDeleteFileNumber` | No | `1` | Minimum threshold for metric `custom-delete-file-number`. Must be `>= 0`. |
| `dataFileMseWeight` | No | `1` | Score weight of `custom-data-file-mse`. Must be `>= 0`. |
| `deleteFileNumberWeight` | No | `100` | Score weight of `custom-delete-file-number`. Must be `>= 0`. |
| `maxPartitionNum` | No | `50` | Maximum number of partitions selected by optimizer. Must be `> 0`. |
| `rewriteOptions` | No | `{}` | Additional rewrite options, expanded as `job.options.*` rules. |

## Generated rules and properties

The policy content is converted to:

- Properties:
  - `strategy.type=compaction`
  - `job.template-name=builtin-iceberg-rewrite-data-files`
- Rules:
  - `trigger-expr=custom-data-file-mse >= minDataFileMse || custom-delete-file-number >= minDeleteFileNumber`
  - `score-expr=custom-data-file-mse * dataFileMseWeight + custom-delete-file-number * deleteFileNumberWeight`
  - `max_partition_num=<maxPartitionNum>`
  - `job.options.<key>=<value>` for each rewrite option

## Parameter tuning guide

### Metric unit and threshold formula

`custom-data-file-mse` is expected to be in `byte^2`.

Use the target file size and a tolerance ratio to set `minDataFileMse`:

`minDataFileMse = (target-file-size-bytes * ratio)^2`

Recommended `ratio` range: `0.1` to `0.2`.

Default values use:

- `target-file-size-bytes = 134217728` (128 MiB)
- `ratio = 0.15`
- `minDataFileMse = 405323966463344`

### Trigger behavior

The trigger expression uses `>=`.

- Set `minDeleteFileNumber = 1` to trigger when at least one delete file exists.
- Set `minDeleteFileNumber > 1` to reduce compaction frequency for delete files.

### Score weights

Score is computed as:

`custom-data-file-mse * dataFileMseWeight + custom-delete-file-number * deleteFileNumberWeight`

- Keep `dataFileMseWeight = 1` as baseline.
- Increase `deleteFileNumberWeight` if you want partitions with more delete files to be prioritized.
- Keep both weights non-negative.

### Recommended defaults for production start

- `minDataFileMse = 405323966463344` (computed from 128 MiB and ratio `0.15`)
- `minDeleteFileNumber = 1`
- `dataFileMseWeight = 1`
- `deleteFileNumberWeight = 100`
- `maxPartitionNum = 50`

Recommended `rewriteOptions`:

- `target-file-size-bytes = 134217728`
- `min-input-files = 5`
- `delete-file-threshold = 1`

## Create policy examples

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "iceberg_compaction_default",
    "comment": "Built-in iceberg compaction policy",
    "policyType": "ICEBERG_COMPACTION",
    "enabled": true,
    "content": {}
  }' \
  http://localhost:8090/api/metalakes/test/policies
```

</TabItem>
<TabItem value="java" label="Java">

```java
GravitinoClient client = ...;

PolicyContent content = PolicyContents.icebergCompaction();

Policy policy =
    client.createPolicy(
        "iceberg_compaction_default",
        "ICEBERG_COMPACTION",
        "Built-in iceberg compaction policy",
        true,
        content);
```

</TabItem>
</Tabs>

## Attach policy to metadata objects

After the policy is created, associate it with a catalog, schema, or table through standard policy association APIs.
The optimizer will read the generated rules and properties to evaluate strategy triggering and job submission context.
