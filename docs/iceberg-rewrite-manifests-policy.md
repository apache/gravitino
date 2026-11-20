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
| `manifest_count_threshold` | `100` | Trigger when manifest count is at least this value. Aligns with Iceberg `commit.manifest.min-count-to-merge`. Must be positive. |
| `spec_id` | Iceberg's current default | Existing partition spec to evaluate. Omission resolves the current default from Iceberg metadata. |
| `use_caching` | Omitted | Optional boolean forwarded to Iceberg; omission preserves the runtime default. |

Spec IDs must be non-negative 32-bit integers. Recommendations are ranked by the selected
spec's manifest count. Average manifest size is still collected for observability but does
not affect the trigger. The policy API accepts only JSON booleans for `use_caching`; numbers
and strings are rejected.

At the default threshold, 99 manifests never trigger and 100 or more trigger regardless of
average manifest size.

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

The recommender and CLI use the table's current default partition spec when `spec_id` is
omitted. The following example uses default thresholds and assumes statistics have been collected:

```shell
curl -X POST -H 'Content-Type: application/json' \
  -d '{
    "name": "rewrite_manifests",
    "policyType": "system_iceberg_rewrite_manifests",
    "enabled": true,
    "content": {}
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

When `spec_id` is omitted, the recommender resolves Iceberg's current `default-spec-id` from
table metadata and retains that value through job submission. TMS leaves an omitted
`spec_id` unset at schedule time and resolves the table's current default when the Spark job
runs. Empty policy content therefore targets the current default spec with the default
thresholds. Set `spec_id` explicitly to target another existing spec, such as an older spec
after partition evolution.

The Iceberg catalog projects `TableMetadata.defaultSpecId()` as the reserved, read-only table
property `default-spec-id`. This is metadata, not an operator setting; it cannot be set or
removed through Gravitino table-property changes. Custom metadata providers must expose the
same value from Iceberg metadata. If an omitted target cannot be resolved because metadata is
missing or invalid, the recommender warns and skips that table without aborting the batch.

The resolved ID is used for both statistics lookup and job submission. Partition evolution
after evaluation cannot retarget an already-created job context. A later evaluation with an
omitted `spec_id` resolves the new default; an explicit policy target remains unchanged.
The handler never infers a spec from statistics map keys. After a snapshot change, recollect
statistics before evaluating again.

Rewriting reorganizes manifests within the selected spec and does not migrate data files
between specs. This policy introduces no scheduling, cooldown, or last-success statistic.
For runtime requirements and direct job submission, see the
[rewrite-manifests job reference](./table-maintenance-service/optimizer-cli-reference.md).
