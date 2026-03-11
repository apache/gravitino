---
title: "Table Maintenance Service (Optimizer)"
slug: /table-maintenance-service/optimizer
keyword: table maintenance, optimizer, statistics, metrics, monitor
license: This software is licensed under the Apache License version 2.
---

## What is this service

The Table Maintenance Service (Optimizer) automates table maintenance by connecting:

- Statistics and metrics collection
- Rule evaluation and strategy recommendation
- Job template based execution

The CLI commands and configuration keys use the `optimizer` name.

## Alpha status and current limitations

The current Table Maintenance Service is in **alpha** stage.

Current limitations:

- It is operated through the optimizer CLI workflow.
- The built-in maintenance strategy focuses on Iceberg table compaction.
- Compaction support is currently limited to Iceberg tables with identity partition transforms.

## Extensibility and roadmap

Although the built-in capability is intentionally narrow in alpha, the framework is designed for
extension:

- Integrate external systems by implementing custom providers and adapters.
- Add new strategies and handlers beyond built-in compaction.
- Plug in custom metrics, evaluators, and job submitters for different environments.

See [Optimizer Extension Guide](./optimizer-extension-guide.md) for extension points.

Future versions will continue improving the out-of-the-box experience and evolve toward a more
ready-to-use maintenance service.

## Architecture overview

The optimizer workflow is based on six parts:

1. Metadata objects: catalog/schema/table in a metalake.
2. Statistics and metrics: table/partition signals used for decision making.
3. Policies: strategy intent, for example `system_iceberg_compaction`.
4. Job templates: executable contracts, for example built-in Spark templates.
5. Job executor: local or custom backend that runs submitted jobs.
6. Status and logs: REST job state plus local staging logs.

Typical data flow:

1. Collect statistics and metrics for target tables.
2. Evaluate rules and produce candidate actions.
3. Submit jobs using a concrete template and `jobConf`.
4. Track status and verify results on table metadata and logs.

## Execution modes

| Mode | Main entry | Best for | Output |
| --- | --- | --- | --- |
| Built-in maintenance workflow | Gravitino REST + built-in templates | Server-side operational runs | Submitted Spark jobs and updated metadata |
| Optimizer CLI local calculator | `gravitino-optimizer.sh` | Local file-driven testing and batch scripts | Statistics/metrics updates and optional submissions |

Use built-in maintenance workflow when you want policy-driven server execution.
Use CLI local calculator when you want to feed JSONL input directly.

## Start here

- Configuration first: read [Optimizer Configuration](./optimizer-configuration.md).
- Need custom integrations: read [Optimizer Extension Guide](./optimizer-extension-guide.md).
- First-time enablement: run [Optimizer Quick Start and Verification](./optimizer-quick-start.md).
- CLI-only usage: read [Optimizer CLI Reference](./optimizer-cli-reference.md).
- Runtime failures or mismatched results: check [Optimizer Troubleshooting](./optimizer-troubleshooting.md).

## Lifecycle

### 1. Collect

Generate or ingest table and partition statistics/metrics.

### 2. Evaluate

Apply policies and rules to decide whether maintenance should run.

### 3. Submit

Pick a job template and submit job with concrete `jobConf`.

### 4. Observe

Check REST job status and validate resulting statistics, metrics, or rewritten data files.

## Configuration model

| Layer | Scope | Typical keys |
| --- | --- | --- |
| Gravitino server config | Runtime for job manager and executor | `gravitino.job.executor`, `gravitino.job.statusPullIntervalInMs`, `gravitino.jobExecutor.local.sparkHome` |
| Job submission `jobConf` | Per job run | `catalog_name`, `table_identifier`, `spark_*`, template-specific args |
| Optimizer CLI config | CLI commands | `gravitino.optimizer.*` in `conf/gravitino-optimizer.conf` |

## Terminology mapping

| Term | Example value | Used in |
| --- | --- | --- |
| Policy name | `iceberg_compaction_default` | Policy identity and CLI `--strategy-name` |
| Policy type | `system_iceberg_compaction` | REST policy creation field `policyType` |
| Strategy type | `iceberg-data-compaction` | Policy content field `strategy.type` and strategy handler config key |

For strategy submission, `--strategy-name` must use policy name, not policy type or strategy type.

## Prerequisites and verification

Quick start prerequisites and success checks are documented in
[Optimizer Quick Start and Verification](./optimizer-quick-start.md).

## Related docs

- [Optimizer Configuration](./optimizer-configuration.md)
- [Optimizer Extension Guide](./optimizer-extension-guide.md)
- [Optimizer Quick Start and Verification](./optimizer-quick-start.md)
- [Optimizer CLI Reference](./optimizer-cli-reference.md)
- [Optimizer Troubleshooting](./optimizer-troubleshooting.md)
- [Manage policies in Gravitino](../manage-policies-in-gravitino.md)
- [Iceberg compaction policy](../iceberg-compaction-policy.md)
- [Manage jobs in Gravitino](../manage-jobs-in-gravitino.md)
- [Manage statistics in Gravitino](../manage-statistics-in-gravitino.md)
