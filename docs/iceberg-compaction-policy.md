---
title: "Iceberg Compaction Policy"
slug: "/iceberg-compaction-policy"
date: 2026-03-05
keyword: "iceberg, compaction, policy, optimizer, Gravitino"
license: "This software is licensed under the Apache License version 2."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Overview

`system_iceberg_compaction` is the built-in policy type used by the optimizer to generate compaction strategies for Iceberg tables. The policy attaches to `CATALOG`, `SCHEMA`, or `TABLE` metadata objects and is the only built-in policy type in alpha.

When the optimizer evaluates a policy, it scores each partition against two signals (file-size variance and delete-file count), then submits a Spark rewrite job for the highest-scoring partitions up to `maxPartitionNum`.

## Policy Content

The typed content for `system_iceberg_compaction` supports the following fields. All are optional; an empty content `{}` accepts the defaults.

| Property | Default | Required | Description |
|---|---|---|---|
| `minDataFileMse` | `405323966463344` | No | Trigger threshold for file-size variance. Compaction fires when `custom-data-file-mse` is at or above this value. The default corresponds to ~15% deviation from a 128 MiB target file size. See [Understanding `custom-data-file-mse`](#understanding-custom-data-file-mse). |
| `minDeleteFileNumber` | `1` | No | Trigger threshold for delete files. Compaction fires when a partition has at least this many delete files. The default `1` triggers on any delete file present. |
| `dataFileMseWeight` | `1` | No | Score multiplier for `custom-data-file-mse`. Raises ranking of partitions with high file-size variance. |
| `deleteFileNumberWeight` | `100` | No | Score multiplier for `custom-delete-file-number`. The default of `100` strongly prioritizes partitions with delete files over partitions that only have file-size variance. |
| `maxPartitionNum` | `50` | No | Maximum partitions selected per evaluation. Caps the number of rewrite jobs submitted per run. |
| `rewriteOptions` | `{}` | No | Iceberg rewrite options passed to the Spark job as `job.options.*`. Set `target-file-size-bytes` here to match your table's `write.target-file-size-bytes` property. |

## Generated Rules and Properties

The policy content is converted to:

- Properties:
  - `strategy.type=iceberg-data-compaction`
  - `job.template-name=builtin-iceberg-rewrite-data-files`
- Rules:
  - `trigger-expr=custom-data-file-mse >= minDataFileMse || custom-delete-file-number >= minDeleteFileNumber`
  - `score-expr=custom-data-file-mse * dataFileMseWeight + custom-delete-file-number * deleteFileNumberWeight`
  - `max-partition-num=<maxPartitionNum>`
  - `job.options.<key>=<value>` for each rewrite option

## Parameter Tuning Guide

### Understanding `custom-data-file-mse`

`custom-data-file-mse` is the mean squared deviation of data file sizes from the target file size in a partition, measured in `byte^2`.

- A partition where every file is exactly the target size has MSE = 0.
- A partition with files significantly smaller or larger than the target has high MSE.
- The metric does not distinguish small-file from large-file problems; both inflate it.

For tuning, it is easier to think in terms of acceptable deviation rather than the raw MSE value. Set the trigger threshold using:

```
minDataFileMse = (target_file_size_bytes × ratio)^2
```

The `ratio` is the fraction of the target size you tolerate as average per-file deviation before compaction fires.

| Ratio | Trigger behavior | Use when |
| --- | --- | --- |
| `0.1` | Files deviate by ~10% from target | Tight file-size discipline, frequent compaction is acceptable |
| `0.15` | Files deviate by ~15% from target (default) | Balanced starting point |
| `0.2` | Files deviate by ~20% from target | Compaction cost is high, occasional small-file accumulation is acceptable |

Default values:

- `target_file_size_bytes = 134217728` (128 MiB)
- `ratio = 0.15`
- `minDataFileMse = (134217728 × 0.15)^2 = 405323966463344`

### Target File Size

The Gravitino default `target-file-size-bytes = 134217728` (128 MiB) does not match the Iceberg project default of 512 MiB or many production deployments. Set `target-file-size-bytes` in `rewriteOptions` to match your table's actual `write.target-file-size-bytes` property:

```json
{
  "rewriteOptions": {
    "target-file-size-bytes": "536870912"
  }
}
```

Common values are 128 MiB (`134217728`), 256 MiB (`268435456`), and 512 MiB (`536870912`). When you change the target, recompute `minDataFileMse` using the formula above so the trigger threshold matches the new target.

### Trigger Behavior

A partition is selected for compaction when either threshold is met:

```
custom-data-file-mse >= minDataFileMse  OR  custom-delete-file-number >= minDeleteFileNumber
```

Both comparisons use `>=`. Set `minDeleteFileNumber = 1` to compact any partition that has even one delete file; raise it to reduce compaction frequency on tables with many small delete operations.

### Score Weights

When more partitions qualify for compaction than `maxPartitionNum` allows, the optimizer ranks partitions by score and selects the top `maxPartitionNum`:

```
score = custom-data-file-mse × dataFileMseWeight + custom-delete-file-number × deleteFileNumberWeight
```

The default weights (`dataFileMseWeight = 1`, `deleteFileNumberWeight = 100`) heavily prioritize partitions with delete files over partitions that only have file-size variance. To tune:

- Keep `dataFileMseWeight = 1` as the baseline.
- Raise `deleteFileNumberWeight` to push delete-heavy partitions further up the queue.
- Lower `deleteFileNumberWeight` if your read patterns are not delete-sensitive and you want file-size variance to dominate ranking.
- Both weights must be non-negative.

### Recommended Defaults for Production Start

Starting policy values:

- `minDataFileMse = 405323966463344` (computed from 128 MiB target and ratio `0.15`)
- `minDeleteFileNumber = 1`
- `dataFileMseWeight = 1`
- `deleteFileNumberWeight = 100`
- `maxPartitionNum = 50`

Starting `rewriteOptions`:

- `target-file-size-bytes = 134217728` (adjust to match your Iceberg `write.target-file-size-bytes`)
- `min-input-files = 5`
- `delete-file-threshold = 1`

### Common Tuning Scenarios

**Many small files, want aggressive compaction:**

- Lower `minDataFileMse` by using ratio `0.1` in the formula.
- Keep `minDeleteFileNumber = 1`.
- Raise `maxPartitionNum` to process more partitions per run.

**Mostly delete files, few small files:**

- Keep `minDataFileMse` at default or raise it.
- Keep `deleteFileNumberWeight = 100` (default) to prioritize delete-heavy partitions.

**Limit compaction frequency:**

- Raise `minDataFileMse` using ratio `0.2` or higher.
- Raise `minDeleteFileNumber` to `5` or `10`.
- Lower `maxPartitionNum` to cap rewrite jobs per run.

**Iceberg target file size is not 128 MiB:**

- Set `rewriteOptions.target-file-size-bytes` to match your table's `write.target-file-size-bytes`.
- Recompute `minDataFileMse` using the formula in [Understanding `custom-data-file-mse`](#understanding-custom-data-file-mse).

## Policy Examples

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "iceberg_compaction_default",
    "comment": "Built-in iceberg compaction policy",
    "policyType": "system_iceberg_compaction",
    "enabled": true,
    "content": {}
  }' \
  http://localhost:8090/api/metalakes/test/policies
```

</TabItem>
<TabItem value="java" label="Java">

```java
GravitinoClient client = ...;

PolicyContent content = PolicyContents.icebergDataCompaction();

Policy policy =
    client.createPolicy(
        "iceberg_compaction_default",
        "system_iceberg_compaction",
        "Built-in iceberg compaction policy",
        true,
        content);
```

</TabItem>
</Tabs>

## Attach Policy to Metadata Objects

After the policy is created, associate it with a catalog, schema, or table through the standard policy-association APIs. The optimizer reads the generated rules and properties to evaluate strategy triggering and the job-submission context.
