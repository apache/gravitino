---
title: "Optimizer CLI Reference"
slug: /table-maintenance-service/cli-reference
keyword: table maintenance, optimizer, cli, commands, metrics, statistics
license: This software is licensed under the Apache License version 2.
---

Use `--help` to list all commands, or `--help --type <command>` for command-specific help.

## Command quick reference

| Command (`--type`) | Required options | Optional options | Purpose |
| --- | --- | --- | --- |
| `submit-strategy-jobs` | `--identifiers`, `--strategy-name` (policy name) | `--dry-run`, `--limit` | Recommend and optionally submit jobs |
| `update-statistics` | `--calculator-name` | `--identifiers`, `--statistics-payload`, `--file-path` | Calculate and persist statistics |
| `append-metrics` | `--calculator-name` | `--identifiers`, `--statistics-payload`, `--file-path` | Calculate and append metrics |
| `monitor-metrics` | `--identifiers`, `--action-time` | `--range-seconds`, `--partition-path` | Evaluate rules with before/after metrics |
| `list-table-metrics` | `--identifiers` | `--partition-path` | Query stored table or partition metrics |
| `list-job-metrics` | `--identifiers` | None | Query stored job metrics |

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
{"stats-type":"table","identifier":"catalog.db.t1","row_count":{"value":100,"timestamp":1735689600}}
```

### Identifier rules

- Table and partition records: `catalog.schema.table`
- If `gravitino.optimizer.gravitinoDefaultCatalog` is set, `schema.table` is also accepted
- Job records: parsed as a regular Gravitino `NameIdentifier`

## CLI workflow examples

### Update statistics in batch

```bash
./bin/gravitino-optimizer.sh \
  --type update-statistics \
  --calculator-name local-stats-calculator \
  --file-path ./table-stats.jsonl \
  --conf-path ./conf/gravitino-optimizer.conf
```

### Append metrics in batch

```bash
./bin/gravitino-optimizer.sh \
  --type append-metrics \
  --calculator-name local-stats-calculator \
  --file-path ./table-stats.jsonl \
  --conf-path ./conf/gravitino-optimizer.conf
```

### Dry-run strategy submission

```bash
./bin/gravitino-optimizer.sh \
  --type submit-strategy-jobs \
  --identifiers rest_catalog.db.t1 \
  --strategy-name iceberg_compaction_default \
  --dry-run \
  --limit 10 \
  --conf-path ./conf/gravitino-optimizer.conf
```

### Monitor metrics

```bash
./bin/gravitino-optimizer.sh \
  --type monitor-metrics \
  --identifiers catalog.db.sales \
  --action-time 1735689600 \
  --range-seconds 86400 \
  --conf-path ./conf/gravitino-optimizer.conf
```

You can configure evaluator rules in `gravitino-optimizer.conf`:

```properties
gravitino.optimizer.monitor.gravitinoMetricsEvaluator.rules = table:row_count:avg:le,job:duration:latest:le
```

Rule format is `scope:metricName:aggregation:comparison`:

- `scope`: `table` or `job` (`table` rules also apply to partition scope)
- `aggregation`: `max|min|avg|latest`
- `comparison`: `lt|le|gt|ge|eq|ne`

## Output guide

- `SUMMARY: ...`: results for `update-statistics` and `append-metrics`
- `DRY-RUN: ...`: recommendation preview without job submission
- `SUBMIT: ...`: job submitted successfully
- `MetricsResult{...}`: returned by list commands
- `EvaluationResult{...}`: returned by monitor command

## Related docs

- [Table Maintenance Service (Optimizer)](./optimizer.md)
- [Optimizer Configuration](./optimizer-configuration.md)
- [Optimizer Quick Start and Verification](./optimizer-quick-start.md)
- [Optimizer Troubleshooting](./optimizer-troubleshooting.md)
