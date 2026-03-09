---
title: "Optimizer CLI Reference"
slug: /table-maintenance-service/cli-reference
keyword: table maintenance, optimizer, cli, commands, metrics, statistics
license: This software is licensed under the Apache License version 2.
---

Use `--help` to list all commands, or `--help --type <command>` for command-specific help.

By default, optimizer CLI loads `conf/gravitino-optimizer.conf` from the current working
directory. Use `--conf-path` only when you need a custom config file.

## Command quick reference

| Command (`--type`) | Required options | Optional options | Purpose |
| --- | --- | --- | --- |
| `submit-strategy-jobs` | `--identifiers`, `--strategy-name` (policy name) | `--dry-run`, `--limit` | Recommend and optionally submit jobs |
| `update-statistics` | `--calculator-name` | `--identifiers`, `--statistics-payload`, `--file-path` | Calculate and persist statistics |
| `append-metrics` | `--calculator-name` | `--identifiers`, `--statistics-payload`, `--file-path` | Calculate and append metrics |
| `monitor-metrics` | `--identifiers`, `--action-time` | `--range-seconds`, `--partition-path` | Evaluate rules with before/after metrics |
| `list-table-metrics` | `--identifiers` | `--partition-path` | Query stored table or partition metrics |
| `list-job-metrics` | `--identifiers` | None | Query stored job metrics |
| `submit-update-stats-job` | `--identifiers` | `--dry-run`, `--update-mode`, `--updater-options`, `--spark-conf` | Submit built-in Iceberg update stats/metrics Spark jobs |

### Option field meanings

| Option | Meaning | Used by |
| --- | --- | --- |
| `--identifiers` | Comma-separated identifiers. Table format supports `catalog.schema.table` (or `schema.table` when default catalog is configured). | Most commands |
| `--strategy-name` | Policy name to evaluate, for example `iceberg_compaction_default`. | `submit-strategy-jobs` |
| `--dry-run` | Preview mode. Prints recommendations or job configs without submitting jobs. | `submit-strategy-jobs`, `submit-update-stats-job` |
| `--limit` | Maximum number of strategy jobs to process. Must be `> 0`. | `submit-strategy-jobs` |
| `--calculator-name` | Statistics/metrics calculator implementation name (for example `local-stats-calculator`). | `update-statistics`, `append-metrics` |
| `--statistics-payload` | Inline JSON Lines content as input. Mutually exclusive with `--file-path`. | `update-statistics`, `append-metrics` |
| `--file-path` | Path to JSON Lines input file. Mutually exclusive with `--statistics-payload`. | `update-statistics`, `append-metrics` |
| `--action-time` | Action timestamp in epoch seconds used as evaluation anchor. | `monitor-metrics` |
| `--range-seconds` | Time window (seconds) for monitor evaluation. Default is `86400` (24h). | `monitor-metrics` |
| `--partition-path` | Partition path JSON array, for example `'[{"dt":"2026-01-01"}]'`. Requires exactly one identifier. | `monitor-metrics`, `list-table-metrics` |
| `--update-mode` | Controls what built-in update job updates: `stats`, `metrics`, or `all` (default). | `submit-update-stats-job` |
| `--updater-options` | Flat JSON map passed to updater logic. For `stats`/`all`, include `gravitino_uri` and `metalake`. | `submit-update-stats-job` |
| `--spark-conf` | Flat JSON map of Spark and Iceberg catalog configs used by the job. | `submit-update-stats-job` |

Global option:

- `--conf-path`: Optional custom config file path. If omitted, CLI uses `conf/gravitino-optimizer.conf`.

## Input format for `local-stats-calculator`

`local-stats-calculator` reads JSON Lines (one JSON object per line).

### Reserved fields

- `stats-type`: `table`, `partition`, or `job`
- `identifier`: object identifier
- `partition-path`: only for partition data, for example `{"dt":"2026-01-01"}`
- `timestamp`: epoch seconds

All other fields are treated as metric or statistic values.

### Supported value forms

Both forms are supported:

```json
{"stats-type":"table","identifier":"catalog.db.t1","row_count":100}
{"stats-type":"table","identifier":"catalog.db.t1","timestamp":1735689600,"row_count":{"value":100}}
```

### Identifier rules

- Table and partition records: `catalog.schema.table`
- If `gravitino.optimizer.gravitinoDefaultCatalog` is set, `schema.table` is also accepted
- Job records: parsed as a regular Gravitino `NameIdentifier`

## CLI workflow examples

### Update statistics in batch

Calculate and persist table or partition statistics from JSONL input.

```bash
./bin/gravitino-optimizer.sh \
  --type update-statistics \
  --calculator-name local-stats-calculator \
  --file-path ./table-stats.jsonl
```

### Append metrics in batch

Calculate and append table or job metrics from JSONL input.

```bash
./bin/gravitino-optimizer.sh \
  --type append-metrics \
  --calculator-name local-stats-calculator \
  --file-path ./table-stats.jsonl
```

### Dry-run strategy submission

Preview recommendations without actually submitting jobs.

```bash
./bin/gravitino-optimizer.sh \
  --type submit-strategy-jobs \
  --identifiers rest_catalog.db.t1 \
  --strategy-name iceberg_compaction_default \
  --dry-run \
  --limit 10
```

### Submit strategy jobs

Submit jobs for identifiers that match the given policy name.

```bash
./bin/gravitino-optimizer.sh \
  --type submit-strategy-jobs \
  --identifiers rest_catalog.db.t1 \
  --strategy-name iceberg_compaction_default \
  --limit 10
```

### Monitor metrics

Evaluate monitor rules around an action time.

```bash
./bin/gravitino-optimizer.sh \
  --type monitor-metrics \
  --identifiers catalog.db.sales \
  --action-time 1735689600 \
  --range-seconds 86400
```

You can configure evaluator rules in `gravitino-optimizer.conf`:

```properties
gravitino.optimizer.monitor.gravitinoMetricsEvaluator.rules = table:row_count:avg:le,job:duration:latest:le
```

Rule format is `scope:metricName:aggregation:comparison`:

- `scope`: `table` or `job` (`table` rules also apply to partition scope)
- `aggregation`: `max|min|avg|latest`
- `comparison`: `lt|le|gt|ge|eq|ne`

### Submit built-in update stats jobs

Submit built-in Iceberg update stats/metrics Spark jobs directly.

```bash
./bin/gravitino-optimizer.sh \
  --type submit-update-stats-job \
  --identifiers rest_catalog.db.t1 \
  --update-mode all \
  --updater-options '{"gravitino_uri":"http://localhost:8090","metalake":"test"}' \
  --spark-conf '{"spark.sql.catalog.rest_catalog.type":"rest","spark.sql.catalog.rest_catalog.uri":"http://localhost:9001/iceberg","spark.hadoop.fs.defaultFS":"file:///"}'
```

Notes:

- `--identifiers` supports `catalog.schema.table` or `schema.table` (when default catalog is configured).
- `--update-mode` supports `stats|metrics|all` (default `all`).
- For `stats` or `all`, `--updater-options` must include `gravitino_uri` and `metalake`.
- `--spark-conf` and `--updater-options` are flat JSON maps.

### List table metrics

Query stored metrics at table scope.

```bash
./bin/gravitino-optimizer.sh \
  --type list-table-metrics \
  --identifiers catalog.db.sales
```

For partition scope, provide a partition path JSON array:

```bash
./bin/gravitino-optimizer.sh \
  --type list-table-metrics \
  --identifiers catalog.db.sales \
  --partition-path '[{"dt":"2026-01-01"}]'
```

### List job metrics

Query stored metrics at job scope.

```bash
./bin/gravitino-optimizer.sh \
  --type list-job-metrics \
  --identifiers catalog.db.optimizer_job
```

## Output guide

- `SUMMARY: ...`: results for `update-statistics` and `append-metrics`
- `DRY-RUN: ...`: recommendation preview without job submission
- `SUBMIT: ...`: job submitted successfully
- `SUMMARY: submit-update-stats-job ...`: summary for built-in update-stats submission
- `MetricsResult{...}`: returned by list commands
- `EvaluationResult{...}`: returned by monitor command

## Related docs

- [Table Maintenance Service (Optimizer)](./optimizer.md)
- [Optimizer Configuration](./optimizer-configuration.md)
- [Optimizer Quick Start and Verification](./optimizer-quick-start.md)
- [Optimizer Troubleshooting](./optimizer-troubleshooting.md)
