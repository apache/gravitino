---
title: "Iceberg Manifest Rewrite Policy"
slug: "/iceberg-rewrite-manifests-policy"
keyword: "iceberg, manifests, policy, optimizer, Gravitino"
license: "This software is licensed under the Apache License version 2."
---

## Behavior

`system_iceberg_rewrite_manifests` evaluates manifest statistics for one resolved partition
spec and submits `builtin-iceberg-rewrite-manifests`. Associate the policy with a tag and
assign that tag to a catalog, schema, or table through the existing
[policy API](./manage-policies-in-gravitino.md). Tables inherit tags from their ancestors.
The strategy type is `iceberg-rewrite-manifests`; its handler and job adapter are built in.

| Content field | Default | Meaning |
| --- | --- | --- |
| `manifest_count_critical` | `500` | Trigger when count is at least this value, regardless of size. |
| `manifest_count_warning` | `100` | Minimum count for the small-average-size trigger. Must be positive. |
| `avg_manifest_size_threshold_bytes` | `8388608` | Trigger when average size is strictly below this value and the warning count is reached. Must be positive. |
| `spec_id` | Omitted | Existing partition spec to evaluate. Required when evaluating previously persisted statistics through the CLI. |
| `use_caching` | Omitted | Optional boolean forwarded to Iceberg; omission preserves the runtime default. |

The critical count must be at least the warning count. Spec IDs must be non-negative
32-bit integers. Thresholds use integer counts and bytes. Recommendations are ranked by
the selected spec's manifest count.

At the defaults, 99 manifests never trigger; 100 manifests averaging 8 MiB do not trigger;
100 averaging less than 8 MiB trigger; and 500 trigger regardless of size.

## Collect, evaluate, and submit

Collect `custom-manifest-number-by-spec` and `custom-avg-manifest-size-by-spec` for the
chosen spec before evaluating. Submit `builtin-iceberg-update-manifest-stats` to collect
this pair without data-file statistics, including after partition evolution. Both values are objects keyed by the decimal spec ID.
The handler reads the same key from one table-statistics response. A missing map or key
means collection is required and produces no recommendation. An empty spec has count and
average zero. Malformed measurements fail evaluation. Counts from other specs are ignored.

For CLI evaluation of persisted statistics, set `spec_id` to the ID used for collection.
The following example assumes that spec `1` exists and its statistics have been collected:

```shell
curl -X POST -H 'Content-Type: application/json' \
  -d '{
    "name": "rewrite_manifests",
    "policyType": "system_iceberg_rewrite_manifests",
    "enabled": true,
    "content": {"spec_id": 1, "use_caching": false}
  }' \
  http://localhost:8090/api/metalakes/test/policies
```

Associate `rewrite_manifests` with a tag using the `ALL_VALUES` selector, then assign that tag
to the table. Configure the optimizer's Gravitino providers and Spark submission settings as described in
[Optimizer configuration](./table-maintenance-service/optimizer-configuration.md), then preview:

```shell
./bin/gravitino-optimizer.sh --type submit-strategy-jobs \
  --identifiers rest_catalog.db.t1 --strategy-name rewrite_manifests --dry-run
```

Remove `--dry-run` to submit the recommendations. The adapter supplies `catalog_name`,
`table_identifier`, the exact evaluated `spec_id`, and optional `use_caching`.
Its catalog, table, and spec values override shared job-submitter configuration.
Recollect statistics after a successful rewrite before evaluating again.

## Keeping the collection target

An application that collects and evaluates in one cycle may omit the requested spec.
`IcebergUpdateManifestStatsJob.collectManifestStatistics(spark, catalog, table, null)`
resolves the current default once and returns an `IcebergManifestStatistics` measurement.
Pass its `statistics()` into `StrategyHandlerContext` and its `specId()` into
`ManifestRewriteStrategyHandler.initialize(context, resolvedSpecId)`. Alternatively, publish
both measurements atomically, read them in one response, and retain that same resolved ID.
An explicitly configured policy spec must match the collection target.

The adapter always submits the resolved ID, even if the table's default changes after collection.
It never infers the default from map keys or resolves it again during evaluation. The standalone
CLI rejects a policy without `spec_id`, because persisted per-spec maps do not identify the
default from an earlier collection cycle.

Rewriting reorganizes manifests within the selected spec and does not migrate data files
between specs. This policy introduces no scheduling, cooldown, or last-success statistic.
For runtime requirements and direct job submission, see the
[rewrite-manifests job reference](./table-maintenance-service/optimizer-cli-reference.md).
