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

package org.apache.gravitino.flink.connector.paimon;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionAlreadyExistsException;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionSpecInvalidException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotPartitionedException;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.Factory;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.flink.connector.PartitionConverter;
import org.apache.gravitino.flink.connector.SchemaAndTablePropertiesConverter;
import org.apache.gravitino.flink.connector.catalog.BaseCatalog;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.flink.FlinkTableFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The GravitinoPaimonCatalog class is an implementation of the BaseCatalog class that is used to
 * proxy the PaimonCatalog class.
 */
public class GravitinoPaimonCatalog extends BaseCatalog {

  private static final Logger LOG = LoggerFactory.getLogger(GravitinoPaimonCatalog.class);

  private final AbstractCatalog paimonCatalog;

  protected GravitinoPaimonCatalog(
      CatalogFactory.Context context,
      String defaultDatabase,
      SchemaAndTablePropertiesConverter schemaAndTablePropertiesConverter,
      PartitionConverter partitionConverter) {
    super(
        context.getName(),
        context.getOptions(),
        defaultDatabase,
        schemaAndTablePropertiesConverter,
        partitionConverter);
    FlinkCatalogFactory flinkCatalogFactory = new FlinkCatalogFactory();
    this.paimonCatalog = flinkCatalogFactory.createCatalog(toPaimonContext(context));
  }

  private static CatalogFactory.Context toPaimonContext(CatalogFactory.Context context) {
    Map<String, String> translatedOptions = new HashMap<>();
    for (Map.Entry<String, String> entry : context.getOptions().entrySet()) {
      String mappedKey =
          PaimonPropertiesConverter.INSTANCE.transformPropertyToFlinkCatalog(entry.getKey());
      // transformPropertyToFlinkCatalog returns null when no mapping exists for a key.
      // Fall back to the original key so Paimon-native properties are not silently dropped.
      translatedOptions.put(mappedKey != null ? mappedKey : entry.getKey(), entry.getValue());
    }
    return new CatalogFactory.Context() {
      @Override
      public String getName() {
        return context.getName();
      }

      @Override
      public Map<String, String> getOptions() {
        return translatedOptions;
      }

      @Override
      public ReadableConfig getConfiguration() {
        return context.getConfiguration();
      }

      @Override
      public ClassLoader getClassLoader() {
        return context.getClassLoader();
      }
    };
  }

  @Override
  protected AbstractCatalog realCatalog() {
    return paimonCatalog;
  }

  @Override
  public Optional<Factory> getFactory() {
    return Optional.of(new FlinkTableFactory());
  }

  @Override
  public CatalogBaseTable getTable(ObjectPath tablePath)
      throws TableNotExistException, CatalogException {
    // Step 1: enforce Gravitino authorization (same path as BaseCatalog.getTable).
    try {
      catalog()
          .asTableCatalog()
          .loadTable(NameIdentifier.of(tablePath.getDatabaseName(), tablePath.getObjectName()));
    } catch (NoSuchTableException e) {
      throw new TableNotExistException(catalogName(), tablePath, e);
    } catch (Exception e) {
      throw new CatalogException(e);
    }
    // Step 2: return the Paimon-native DataCatalogTable (with proper CatalogEnvironment)
    // so that AddPartitionCommitCallback is registered during write.
    return paimonCatalog.getTable(tablePath);
  }

  // ---------------------------------------------------------------------------
  // Partition DDL
  //
  // BaseCatalog throws UnsupportedOperationException for all three methods.
  // Override and delegate to paimonCatalog so that the live HiveCatalog inside
  // paimonCatalog can propagate partition changes to Hive metastore.
  //
  // Note: Gravitino does not currently enforce partition-level authorization,
  // so routing partition DDL directly to paimonCatalog does not bypass any
  // Gravitino authz checks that would otherwise apply.
  // ---------------------------------------------------------------------------

  @Override
  public void createPartition(
      ObjectPath tablePath,
      CatalogPartitionSpec partitionSpec,
      CatalogPartition partition,
      boolean ignoreIfExists)
      throws TableNotExistException, TableNotPartitionedException, PartitionSpecInvalidException,
          PartitionAlreadyExistsException, CatalogException {
    paimonCatalog.createPartition(tablePath, partitionSpec, partition, ignoreIfExists);
  }

  @Override
  public void dropPartition(
      ObjectPath tablePath, CatalogPartitionSpec partitionSpec, boolean ignoreIfNotExists)
      throws PartitionNotExistException, CatalogException {
    paimonCatalog.dropPartition(tablePath, partitionSpec, ignoreIfNotExists);
  }

  @Override
  public void alterPartition(
      ObjectPath tablePath,
      CatalogPartitionSpec partitionSpec,
      CatalogPartition newPartition,
      boolean ignoreIfNotExists)
      throws PartitionNotExistException, CatalogException {
    paimonCatalog.alterPartition(tablePath, partitionSpec, newPartition, ignoreIfNotExists);
  }

  /**
   * Creates the table in Paimon/Hive metastore first so that it is visible to other Hive-aware
   * engines, then persists the metadata in Gravitino. Gravitino authorization is enforced by the
   * {@code super.createTable()} call.
   *
   * <p>If the Gravitino call fails after Paimon has already created the table, the Paimon-side
   * creation is rolled back via {@code paimonCatalog.dropTable} so the two stores remain
   * consistent. If the rollback itself fails, the inconsistency is logged at ERROR level so
   * operators can detect and remediate it.
   */
  @Override
  public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists)
      throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
    try {
      paimonCatalog.createTable(tablePath, table, ignoreIfExists);
    } catch (Exception e) {
      throw new CatalogException(
          "Failed to create table in Paimon/Hive metastore: " + tablePath, e);
    }
    try {
      super.createTable(tablePath, table, ignoreIfExists);
    } catch (TableAlreadyExistException | DatabaseNotExistException e) {
      rollbackPaimonCreate(tablePath);
      throw e;
    } catch (Exception e) {
      rollbackPaimonCreate(tablePath);
      throw new CatalogException(
          "Failed to register table in Gravitino (Paimon-side creation rolled back): " + tablePath,
          e);
    }
  }

  private void rollbackPaimonCreate(ObjectPath tablePath) {
    try {
      paimonCatalog.dropTable(tablePath, true);
    } catch (Exception rollbackEx) {
      LOG.error(
          "Rollback failed: table {} was created in Paimon/Hive metastore but Gravitino "
              + "rejected it and the Paimon-side cleanup also failed. Manual cleanup required.",
          tablePath,
          rollbackEx);
    }
  }

  /**
   * Syncs the table change to Paimon/Hive metastore first, then persists the change in Gravitino.
   *
   * <p>If the table is absent from Paimon the method returns early (without updating Gravitino) to
   * prevent the two stores from diverging. Gravitino authorization is enforced by the {@code
   * super.alterTable()} call.
   */
  @Override
  public void alterTable(ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists)
      throws TableNotExistException, CatalogException {
    if (!syncAlterToPaimon(
        tablePath,
        ignoreIfNotExists,
        () -> paimonCatalog.alterTable(tablePath, newTable, ignoreIfNotExists))) {
      return;
    }
    super.alterTable(tablePath, newTable, ignoreIfNotExists);
  }

  /**
   * Same contract as {@link #alterTable(ObjectPath, CatalogBaseTable, boolean)} but accepts an
   * explicit list of {@link TableChange}.
   */
  @Override
  public void alterTable(
      ObjectPath tablePath,
      CatalogBaseTable newTable,
      List<TableChange> tableChanges,
      boolean ignoreIfNotExists)
      throws TableNotExistException, CatalogException {
    if (!syncAlterToPaimon(
        tablePath,
        ignoreIfNotExists,
        () -> paimonCatalog.alterTable(tablePath, newTable, tableChanges, ignoreIfNotExists))) {
      return;
    }
    super.alterTable(tablePath, newTable, tableChanges, ignoreIfNotExists);
  }

  /**
   * Executes {@code paimonOp} and returns {@code true} when the caller should proceed with the
   * Gravitino update, or {@code false} when it should return early.
   *
   * <p>Returns {@code false} when the table does not exist in Paimon and {@code
   * ignoreIfNotExists=true} — proceeding to Gravitino alone would diverge the two stores.
   */
  private boolean syncAlterToPaimon(
      ObjectPath tablePath, boolean ignoreIfNotExists, PaimonAlterOp paimonOp)
      throws TableNotExistException, CatalogException {
    if (!paimonCatalog.tableExists(tablePath)) {
      if (!ignoreIfNotExists) {
        throw new TableNotExistException(catalogName(), tablePath);
      }
      return false;
    }
    try {
      paimonOp.run();
    } catch (TableNotExistException e) {
      if (!ignoreIfNotExists) {
        throw new CatalogException(
            "Failed to sync Paimon/Hive metastore for table: " + tablePath, e);
      }
      return false;
    } catch (CatalogException e) {
      throw e;
    } catch (Exception e) {
      throw new CatalogException("Failed to sync Paimon/Hive metastore for table: " + tablePath, e);
    }
    return true;
  }

  @FunctionalInterface
  private interface PaimonAlterOp {
    void run() throws Exception;
  }

  @Override
  public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists)
      throws TableNotExistException, CatalogException {
    boolean dropped =
        catalog()
            .asTableCatalog()
            .purgeTable(NameIdentifier.of(tablePath.getDatabaseName(), tablePath.getObjectName()));
    if (!dropped && !ignoreIfNotExists) {
      throw new TableNotExistException(catalogName(), tablePath);
    }
    if (dropped) {
      try {
        paimonCatalog.dropTable(tablePath, true);
      } catch (Exception e) {
        throw new CatalogException(
            "Gravitino metadata dropped, but failed to sync Paimon/Hive metastore for table: "
                + tablePath,
            e);
      }
    }
  }
}
