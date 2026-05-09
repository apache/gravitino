---
title: "Optimizer Extension Guide"
slug: /table-maintenance-service/extension-guide
keyword: table maintenance, optimizer, extension, provider, ServiceLoader
license: This software is licensed under the Apache License version 2.
---

Use this guide when built-in optimizer components do not match your environment and you need custom implementations.

## Extension model

Optimizer supports three loading patterns:

1. `Provider` SPI (`name()` + `initialize()`): loaded by `ServiceLoader` and selected by config value.
2. Class-name mapping for strategy handlers and job adapters.
3. Typed SPI for `StatisticsCalculator` and `MetricsEvaluator`.

## Extension points and config keys

| Area | Interface / type | Config key | Loading mode |
| --- | --- | --- | --- |
| Recommender statistics | `StatisticsProvider` | `gravitino.optimizer.recommender.statisticsProvider` | `Provider` SPI by `name()` |
| Recommender strategy source | `StrategyProvider` | `gravitino.optimizer.recommender.strategyProvider` | `Provider` SPI by `name()` |
| Recommender table metadata | `TableMetadataProvider` | `gravitino.optimizer.recommender.tableMetaProvider` | `Provider` SPI by `name()` |
| Recommender job submission | `JobSubmitter` | `gravitino.optimizer.recommender.jobSubmitter` | `Provider` SPI by `name()` |
| Strategy evaluation logic | `StrategyHandler` | `gravitino.optimizer.strategyHandler.<strategyType>.className` | Reflection by class name |
| Job template adaptation | `GravitinoJobAdapter` | `gravitino.optimizer.jobAdapter.<jobTemplate>.className` | Reflection by class name |
| Update statistics sink | `StatisticsUpdater` | `gravitino.optimizer.updater.statisticsUpdater` | `Provider` SPI by `name()` |
| Update metrics sink | `MetricsUpdater` | `gravitino.optimizer.updater.metricsUpdater` | `Provider` SPI by `name()` |
| Monitor metrics source | `MetricsProvider` | `gravitino.optimizer.monitor.metricsProvider` | `Provider` SPI by `name()` |
| Monitor table-job relation | `TableJobRelationProvider` | `gravitino.optimizer.monitor.tableJobRelationProvider` | `Provider` SPI by `name()` |
| Monitor evaluator | `MetricsEvaluator` | `gravitino.optimizer.monitor.metricsEvaluator` | Typed SPI (`ServiceLoader<MetricsEvaluator>`) |
| Monitor callbacks | `MonitorCallback` | `gravitino.optimizer.monitor.callbacks` | `Provider` SPI by `name()` (comma-separated) |
| CLI calculator | `StatisticsCalculator` | CLI `--calculator-name` | Typed SPI (`ServiceLoader<StatisticsCalculator>`) |

## Implement a custom provider

Most extension points use `Provider`:

```java
public class MyStatisticsProvider implements StatisticsProvider {
  @Override
  public String name() {
    return "my-statistics-provider";
  }

  @Override
  public void initialize(OptimizerEnv optimizerEnv) {
    // Initialize clients/resources from optimizer config.
  }

  @Override
  public void close() throws Exception {}
}
```

Requirements:

- Keep a stable `name()` value; config resolves by this name (case-insensitive).
- Provide a public no-arg constructor.
- Implement `initialize(OptimizerEnv)` and `close()` lifecycle correctly.

## Register with ServiceLoader

### For `Provider` implementations

Create file:

`META-INF/services/org.apache.gravitino.maintenance.optimizer.api.common.Provider`

Add your implementation class name per line:

```text
com.example.optimizer.MyStatisticsProvider
com.example.optimizer.MyJobSubmitter
```

### For `StatisticsCalculator`

Create file:

`META-INF/services/org.apache.gravitino.maintenance.optimizer.api.updater.StatisticsCalculator`

### For `MetricsEvaluator`

Create file:

`META-INF/services/org.apache.gravitino.maintenance.optimizer.api.monitor.MetricsEvaluator`

## Configure `gravitino-optimizer.conf`

```properties
gravitino.optimizer.recommender.statisticsProvider = my-statistics-provider
gravitino.optimizer.recommender.jobSubmitter = my-job-submitter

gravitino.optimizer.strategyHandler.my-strategy.className = com.example.optimizer.MyStrategyHandler
gravitino.optimizer.jobAdapter.my-job-template.className = com.example.optimizer.MyJobAdapter

gravitino.optimizer.monitor.metricsEvaluator = my-metrics-evaluator
```

Notes:

- `strategyHandler.<strategyType>.className` must match `strategy.type` in policy content.
- `jobAdapter.<jobTemplate>.className` must match the target job template name.
- `jobSubmitterConfig.*` entries are passed to job submitters as shared runtime options.

## Package and deploy

- Build a JAR containing your classes and `META-INF/services` files.
- Put the JAR on optimizer runtime classpath, for example `${GRAVITINO_HOME}/optimizer/libs/`.
- Restart optimizer process before testing.

If you also extend Gravitino server job execution, see [Manage jobs in Gravitino](../manage-jobs-in-gravitino.md).

## Validation checklist

1. `--help` shows no load-time SPI errors.
2. Commands using your extension run without `No ... found for provider name` errors.
3. Strategy flow can resolve both handler and job adapter mappings.
4. Dry-run (`submit-strategy-jobs --dry-run`) prints expected recommendations.

## Related docs

- [Table Maintenance Service (Optimizer)](./optimizer.md)
- [Optimizer Configuration](./optimizer-configuration.md)
- [Optimizer CLI Reference](./optimizer-cli-reference.md)
- [Optimizer Troubleshooting](./optimizer-troubleshooting.md)
