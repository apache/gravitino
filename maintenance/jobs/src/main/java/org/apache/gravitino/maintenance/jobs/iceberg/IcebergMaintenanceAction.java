/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.maintenance.jobs.iceberg;

import java.util.Map;
import javax.annotation.Nullable;
import org.apache.spark.sql.SparkSession;

/**
 * TMS maintenance contract implemented by the four Iceberg built-in jobs.
 *
 * <p>CLI entry points keep their typed {@code execute} / {@code main} APIs. This interface is the
 * shared orchestration surface used by {@link IcebergMaintenanceJob}.
 *
 * <p>Methods are declared in pipeline order: gate → collect S0 → drift check → execute + S1.
 */
public interface IcebergMaintenanceAction {

  /**
   * Whether this action has an S0 before-metrics step. Orphan cleanup returns {@code false}.
   *
   * @return {@code true} when {@link #collectBeforeMetrics} should run
   */
  boolean hasBeforeMetrics();

  /**
   * Collects S0 before-metrics. Must not be called when {@link #hasBeforeMetrics()} is {@code
   * false}.
   *
   * @param spark active Spark session
   * @param catalogName Iceberg catalog name
   * @param tableIdentifier fully qualified table name
   * @return before-metrics sample
   */
  TableSnapshotMetrics collectBeforeMetrics(
      SparkSession spark, String catalogName, String tableIdentifier);

  /**
   * Whether a changed HEAD snapshot after S0 should skip the procedure. Compaction and manifest
   * rewrite return {@code true}; expiry and orphan cleanup return {@code false}.
   *
   * @return {@code true} when snapshot drift should skip execution
   */
  boolean shouldCheckSnapshotDrift();

  /**
   * Runs the Iceberg procedure and returns S1 after-metrics.
   *
   * <p>Kept as one method because S1 sources differ by type (table inventory, procedure rows, or
   * in-procedure counters); splitting would need an extra outcome carrier.
   *
   * @param spark active Spark session
   * @param catalogName Iceberg catalog name
   * @param tableIdentifier fully qualified table name
   * @param argsByName unresolved job arguments (placeholders already present as raw values)
   * @param before S0 sample when {@link #hasBeforeMetrics()} is {@code true}; otherwise {@code
   *     null}
   * @return after-metrics sample
   * @throws Exception if the procedure fails
   */
  TableSnapshotMetrics executeAndCollectAfter(
      SparkSession spark,
      String catalogName,
      String tableIdentifier,
      Map<String, String> argsByName,
      @Nullable TableSnapshotMetrics before)
      throws Exception;
}
