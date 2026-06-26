/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.iceberg.hive;

import com.google.common.base.Preconditions;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hadoop.ConfigProperties;
import org.apache.thrift.TException;

/** HMS metastore pointer swap used by {@link org.apache.iceberg.RegisterTableOverwrite}. */
public final class HiveRegisterTablePointerSwap {

  private HiveRegisterTablePointerSwap() {}

  /**
   * Atomically updates the HMS metadata location for an existing Iceberg table registration.
   *
   * @param catalog hive catalog that owns the table
   * @param tableIdentifier table identifier to update
   * @param oldMetadataLocation expected current metadata location
   * @param newMetadataLocation metadata location to register
   */
  public static void swap(
      HiveCatalog catalog,
      TableIdentifier tableIdentifier,
      String oldMetadataLocation,
      String newMetadataLocation) {
    HiveTableOperations ops = (HiveTableOperations) catalog.newTableOps(tableIdentifier);
    HiveOperationsBase hiveOps = ops;
    Configuration conf = catalog.getConf();

    TableMetadata base = ops.current();
    TableMetadata targetMetadata =
        TableMetadataParser.read(ops.io().newInputFile(newMetadataLocation));

    boolean hiveEngineEnabled = hiveEngineEnabled(targetMetadata, conf);
    boolean keepHiveStats = conf.getBoolean(ConfigProperties.KEEP_HIVE_STATS, false);

    Set<String> removedProps =
        base.properties().keySet().stream()
            .filter(key -> !targetMetadata.properties().containsKey(key))
            .collect(Collectors.toSet());

    Preconditions.checkArgument(
        !removedProps.contains(TableProperties.ENCRYPTION_TABLE_KEY),
        "Cannot remove key ID from an encrypted table");
    Preconditions.checkArgument(
        Objects.equals(
            base.properties().get(TableProperties.ENCRYPTION_TABLE_KEY),
            targetMetadata.properties().get(TableProperties.ENCRYPTION_TABLE_KEY)),
        "Cannot modify key ID of an encrypted table");

    HiveLock lock = ops.lockObject(base);
    try {
      lock.lock();

      Table tbl = hiveOps.loadHmsTable();
      if (tbl == null) {
        throw new NoSuchTableException("Table does not exist: %s", tableIdentifier);
      }

      HiveOperationsBase.validateTableIsIceberg(tbl, tableIdentifier.toString());

      tbl.setSd(
          HiveOperationsBase.storageDescriptor(
              targetMetadata.schema(), targetMetadata.location(), hiveEngineEnabled));

      String hmsMetadataLocation =
          tbl.getParameters().get(BaseMetastoreTableOperations.METADATA_LOCATION_PROP);
      if (!Objects.equals(oldMetadataLocation, hmsMetadataLocation)) {
        throw new CommitFailedException(
            "Cannot overwrite table %s: metadata location %s has changed from %s",
            tableIdentifier, hmsMetadataLocation, oldMetadataLocation);
      }

      HMSTablePropertyHelper.updateHmsTableForIcebergTable(
          newMetadataLocation,
          tbl,
          targetMetadata,
          removedProps,
          hiveEngineEnabled,
          hiveOps.maxHiveTablePropertySize(),
          oldMetadataLocation);

      if (!keepHiveStats) {
        tbl.getParameters().remove(StatsSetupConst.COLUMN_STATS_ACCURATE);
        tbl.getParameters().put(StatsSetupConst.DO_NOT_UPDATE_STATS, StatsSetupConst.TRUE);
      }

      lock.ensureActive();
      hiveOps.persistTable(tbl, true, hiveLockEnabled(base, conf) ? null : oldMetadataLocation);
      lock.ensureActive();
    } catch (LockException e) {
      throw new CommitStateUnknownException(
          String.format(
              "Failed to heartbeat for hive lock while overwriting table registration for %s",
              tableIdentifier),
          e);
    } catch (TException e) {
      throw new RuntimeException(
          String.format(
              "Metastore operation failed while overwriting table registration for %s",
              tableIdentifier),
          e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted while overwriting table registration", e);
    } finally {
      lock.unlock();
    }
  }

  private static boolean hiveEngineEnabled(TableMetadata metadata, Configuration conf) {
    if (metadata.properties().get(TableProperties.ENGINE_HIVE_ENABLED) != null) {
      return metadata.propertyAsBoolean(TableProperties.ENGINE_HIVE_ENABLED, false);
    }

    return conf.getBoolean(
        ConfigProperties.ENGINE_HIVE_ENABLED, TableProperties.ENGINE_HIVE_ENABLED_DEFAULT);
  }

  private static boolean hiveLockEnabled(TableMetadata metadata, Configuration conf) {
    if (metadata != null && metadata.properties().get(TableProperties.HIVE_LOCK_ENABLED) != null) {
      return metadata.propertyAsBoolean(TableProperties.HIVE_LOCK_ENABLED, false);
    }

    return conf.getBoolean(
        ConfigProperties.LOCK_HIVE_ENABLED, TableProperties.HIVE_LOCK_ENABLED_DEFAULT);
  }
}
