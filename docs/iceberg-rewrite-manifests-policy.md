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
| `spec_id` | Omitted | Existing partition spec to evaluate. Required by the recommender and CLI; omission skips the policy. |
| `use_caching` | Omitted | Optional boolean forwarded to Iceberg; omission preserves the runtime default. |

The critical count must be at least the warning count. Spec IDs must be non-negative
32-bit integers. Thresholds use integer counts and bytes. Recommendations are ranked by
the selected spec's manifest count. The policy API accepts only JSON booleans for
`use_caching`; numbers and strings are rejected.

At the defaults, 99 manifests never trigger; 100 manifests averaging 8 MiB do not trigger;
100 averaging less than 8 MiB trigger; and 500 trigger regardless of size.

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
The recommender reads metadata after statistics to detect snapshot changes during collection
of its inputs. This freshness check is not transactional with job submission; the table may
still change after evaluation.

The handler reads the chosen spec's count and average from the same statistics response.
A missing map or key produces no recommendation. An empty spec has count and average zero.
Invalid policy rules or malformed measurements are logged and skipped for that table without
aborting recommendations for other tables. Counts from other specs are ignored.

For recommender or CLI evaluation, set `spec_id` in the policy to the existing spec to evaluate.
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
Its catalog, table, spec, and caching values override shared job-submitter configuration.
When the policy omits `use_caching`, the adapter supplies an empty option to clear any shared
caching setting and leave the Iceberg runtime default in effect.
Recollect statistics after a successful rewrite before evaluating again.

## Selecting a partition spec

The recommender and CLI require an explicit policy `spec_id`. They do not resolve the table's
current default spec or receive a target spec from the collector. There is no automatic
collection-to-recommendation handoff in this release. Empty policy content is accepted for
storing default thresholds, but the recommender skips it with a warning until `spec_id` is set.

Choose an existing spec before configuring the policy. To target the current default, read it
from the table's Iceberg metadata and store that ID as `spec_id`. The adapter submits the exact
evaluated ID even if the table's default changes afterward. It never infers a spec from the
statistics map keys. After a snapshot change, recollect the statistics and evaluate the same
configured spec again.

Rewriting reorganizes manifests within the selected spec and does not migrate data files
between specs. This policy introduces no scheduling, cooldown, or last-success statistic.
For runtime requirements and direct job submission, see the
[rewrite-manifests job reference](./table-maintenance-service/optimizer-cli-reference.md).
