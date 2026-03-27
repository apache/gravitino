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

import java.util.List;
import java.util.Optional;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.Factory;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.flink.connector.PartitionConverter;
import org.apache.gravitino.flink.connector.SchemaAndTablePropertiesConverter;
import org.apache.gravitino.flink.connector.catalog.BaseCatalog;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.flink.FlinkTableFactory;

/**
 * The GravitinoPaimonCatalog class is an implementation of the BaseCatalog class that is used to
 * proxy the PaimonCatalog class.
 */
public class GravitinoPaimonCatalog extends BaseCatalog {

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
    this.paimonCatalog = flinkCatalogFactory.createCatalog(context);
  }

  @Override
  protected AbstractCatalog realCatalog() {
    return paimonCatalog;
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
            "Gravitino metadata dropped, but failed to sync Paimon/Hive metastore"
                + " for table: "
                + tablePath,
            e);
      }
    }
  }

  @Override
  public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists)
      throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
    super.createTable(tablePath, table, ignoreIfExists);
    try {
      if (!paimonCatalog.tableExists(tablePath)) {
        paimonCatalog.createTable(tablePath, table, ignoreIfExists);
      }
    } catch (Exception e) {
      throw new CatalogException(
          "Gravitino metadata written, but failed to sync Paimon/Hive metastore for table "
              + tablePath,
          e);
    }
  }

  @Override
  public void alterTable(ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists)
      throws TableNotExistException, CatalogException {
    super.alterTable(tablePath, newTable, ignoreIfNotExists);

    try {
      if (paimonCatalog.tableExists(tablePath)) {
        paimonCatalog.alterTable(tablePath, newTable, ignoreIfNotExists);
      }
    } catch (Exception e) {
      throw new CatalogException(
          "Gravitino metadata updated, but failed to sync Paimon/Hive metastore for table "
              + tablePath,
          e);
    }
  }

  @Override
  public void alterTable(
      ObjectPath tablePath,
      CatalogBaseTable newTable,
      List<TableChange> tableChanges,
      boolean ignoreIfNotExists)
      throws TableNotExistException, CatalogException {
    super.alterTable(tablePath, newTable, tableChanges, ignoreIfNotExists);

    try {
      if (paimonCatalog.tableExists(tablePath)) {
        paimonCatalog.alterTable(tablePath, newTable, tableChanges, ignoreIfNotExists);
      }
    } catch (Exception e) {
      throw new CatalogException(
          "Gravitino metadata updated, but failed to sync Paimon/Hive metastore for table "
              + tablePath,
          e);
    }
  }

  @Override
  public Optional<Factory> getFactory() {
    return Optional.of(new FlinkTableFactory());
  }
}
