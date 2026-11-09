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

import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.spark.sql.SparkSession;

/** Snapshot helpers shared by TMS Iceberg maintenance actions. */
final class IcebergMaintenanceSnapshots {

  private IcebergMaintenanceSnapshots() {}

  static long requireCurrentId(SparkSession spark, String catalogName, String tableIdentifier) {
    Snapshot snapshot = load(spark, catalogName, tableIdentifier).currentSnapshot();
    if (snapshot == null) {
      throw new IllegalStateException(
          "Iceberg table has no current snapshot: " + catalogName + "." + tableIdentifier);
    }
    return snapshot.snapshotId();
  }

  /** After expiry/orphan, HEAD may briefly be empty; report {@code -1} in that case. */
  static long currentIdOrEmpty(SparkSession spark, String catalogName, String tableIdentifier) {
    Snapshot snapshot = load(spark, catalogName, tableIdentifier).currentSnapshot();
    return snapshot == null ? -1L : snapshot.snapshotId();
  }

  private static Table load(SparkSession spark, String catalogName, String tableIdentifier) {
    // Catch Exception (not Spark NoSuchTableException) so the Gravitino server can load callers
    // without Spark catalyst on the classpath.
    try {
      return Spark3Util.loadIcebergTable(
          spark, String.format("%s.%s", catalogName, tableIdentifier));
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new IllegalStateException(
          "Failed to load Iceberg table for " + catalogName + "." + tableIdentifier, e);
    }
  }
}
