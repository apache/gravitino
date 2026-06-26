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
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.iceberg.common.ClosableHiveCatalog;
import org.apache.gravitino.iceberg.common.cache.SupportsMetadataLocation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.ClientPool;
import org.apache.iceberg.MetastoreRegisterTableUtils;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hadoop.ConfigProperties;
import org.apache.thrift.TException;

public class HiveCatalogWithMetadataLocationSupport extends ClosableHiveCatalog
    implements SupportsMetadataLocation {
  private ClientPool<IMetaStoreClient, TException> metaClients;

  @Override
  public void initialize(String name, Map<String, String> properties) {
    super.initialize(name, properties);
    loadFields();
  }

  /**
   * Registers a table from an existing metadata file, optionally overwriting an existing
   * registration.
   *
   * @param identifier table identifier to register
   * @param metadataFileLocation location of the metadata file to register
   * @param overwrite whether to overwrite an existing table registration
   * @return the registered table
   */
  @Override
  public Table registerTable(
      TableIdentifier identifier, String metadataFileLocation, boolean overwrite) {
    return MetastoreRegisterTableUtils.registerTable(
        this, identifier, metadataFileLocation, overwrite, this::overwriteMetadataLocation);
  }

  @Override
  public String metadataLocation(TableIdentifier tableIdentifier) {
    String dbName = tableIdentifier.namespace().level(0);
    String tableName = tableIdentifier.name();

    try {
      org.apache.hadoop.hive.metastore.api.Table hiveTable =
          metaClients.run(client -> client.getTable(dbName, tableName));
      String tableType =
          hiveTable.getParameters().get(BaseMetastoreTableOperations.TABLE_TYPE_PROP);
      if (tableType == null
          || !tableType.equalsIgnoreCase(BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE)) {
        return null;
      }
      return hiveTable.getParameters().get(METADATA_LOCATION_PROP);
    } catch (Exception e) {
      return null;
    }
  }

  private void overwriteMetadataLocation(
      TableIdentifier tableIdentifier,
      String expectedMetadataLocation,
      String newMetadataLocation) {
    HiveTableOperations ops = (HiveTableOperations) newTableOps(tableIdentifier);
    HiveOperationsBase hiveOps = ops;
    Configuration conf = getConf();

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
        "Cannot remove key ID of an encrypted table");

    HiveLock lock = ops.lockObject(base);
    try {
      lock.lock();

      org.apache.hadoop.hive.metastore.api.Table tbl = hiveOps.loadHmsTable();
      if (tbl == null) {
        throw new NoSuchTableException("Table does not exist: %s", tableIdentifier);
      }

      HiveOperationsBase.validateTableIsIceberg(tbl, tableIdentifier.toString());

      tbl.setSd(
          HiveOperationsBase.storageDescriptor(
              targetMetadata.schema(), targetMetadata.location(), hiveEngineEnabled));

      String hmsMetadataLocation =
          tbl.getParameters().get(BaseMetastoreTableOperations.METADATA_LOCATION_PROP);
      if (!Objects.equals(expectedMetadataLocation, hmsMetadataLocation)) {
        throw new CommitFailedException(
            "Cannot overwrite table %s: metadata location %s has changed from %s",
            tableIdentifier, hmsMetadataLocation, expectedMetadataLocation);
      }

      HMSTablePropertyHelper.updateHmsTableForIcebergTable(
          newMetadataLocation,
          tbl,
          targetMetadata,
          removedProps,
          hiveEngineEnabled,
          hiveOps.maxHiveTablePropertySize(),
          expectedMetadataLocation);

      if (!keepHiveStats) {
        tbl.getParameters().remove(StatsSetupConst.COLUMN_STATS_ACCURATE);
        tbl.getParameters().put(StatsSetupConst.DO_NOT_UPDATE_STATS, StatsSetupConst.TRUE);
      }

      lock.ensureActive();
      hiveOps.persistTable(
          tbl, true, hiveLockEnabled(base, conf) ? null : expectedMetadataLocation);
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

  private void loadFields() {
    try {
      this.metaClients =
          (ClientPool<IMetaStoreClient, TException>) FieldUtils.readField(this, "clients", true);
      Preconditions.checkState(
          metaClients != null, "Failed to get clients field from hive catalog");
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }
}
