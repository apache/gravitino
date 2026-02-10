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
package org.apache.gravitino.flink.connector.hive;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogPropertiesUtil;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogBaseTable;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.factories.Factory;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;
import org.apache.gravitino.flink.connector.PartitionConverter;
import org.apache.gravitino.flink.connector.SchemaAndTablePropertiesConverter;
import org.apache.gravitino.flink.connector.catalog.BaseCatalog;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.hadoop.hive.conf.HiveConf;

/**
 * The GravitinoHiveCatalog class is an implementation of the BaseCatalog class that is used to
 * proxy the HiveCatalog class.
 */
public class GravitinoHiveCatalog extends BaseCatalog {

  private HiveCatalog hiveCatalog;

  GravitinoHiveCatalog(
      String catalogName,
      String defaultDatabase,
      Map<String, String> catalogOptions,
      SchemaAndTablePropertiesConverter schemaAndTablePropertiesConverter,
      PartitionConverter partitionConverter,
      @Nullable HiveConf hiveConf,
      @Nullable String hiveVersion) {
    super(
        catalogName,
        catalogOptions,
        defaultDatabase,
        schemaAndTablePropertiesConverter,
        partitionConverter);
    this.hiveCatalog = new HiveCatalog(catalogName, defaultDatabase, hiveConf, hiveVersion);
  }

  public HiveConf getHiveConf() {
    return hiveCatalog.getHiveConf();
  }

  @Override
  public Optional<Factory> getFactory() {
    return hiveCatalog.getFactory();
  }

  @Override
  protected AbstractCatalog realCatalog() {
    return hiveCatalog;
  }

  @Override
  public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists)
      throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
    Preconditions.checkArgument(
        table instanceof ResolvedCatalogBaseTable, "table should be resolved");

    if (!FlinkGenericTableUtil.isGenericTableWhenCreate(table.getOptions())) {
      super.createTable(tablePath, table, ignoreIfExists);
      return;
    }

    if (!(table instanceof ResolvedCatalogTable)) {
      throw new CatalogException("Generic table must be a resolved catalog table");
    }
    ResolvedCatalogTable resolvedTable = (ResolvedCatalogTable) table;

    NameIdentifier identifier =
        NameIdentifier.of(tablePath.getDatabaseName(), tablePath.getObjectName());
    Map<String, String> properties =
        FlinkGenericTableUtil.toGravitinoGenericTableProperties(resolvedTable);

    try {
      catalog()
          .asTableCatalog()
          .createTable(
              identifier,
              new Column[0],
              table.getComment(),
              properties,
              new Transform[0],
              Distributions.NONE,
              new SortOrder[0],
              new Index[0]);
    } catch (NoSuchSchemaException e) {
      throw new DatabaseNotExistException(catalogName(), tablePath.getDatabaseName(), e);
    } catch (TableAlreadyExistsException e) {
      if (!ignoreIfExists) {
        throw new TableAlreadyExistException(catalogName(), tablePath, e);
      }
    } catch (Exception e) {
      throw new CatalogException(e);
    }
  }

  @Override
  public CatalogBaseTable getTable(ObjectPath tablePath)
      throws TableNotExistException, CatalogException {
    try {
      Table table =
          catalog()
              .asTableCatalog()
              .loadTable(NameIdentifier.of(tablePath.getDatabaseName(), tablePath.getObjectName()));
      if (FlinkGenericTableUtil.isGenericTableWhenLoad(table.properties())) {
        return FlinkGenericTableUtil.toFlinkGenericTable(table);
      }
      return super.toFlinkTable(table, tablePath);
    } catch (NoSuchTableException e) {
      throw new TableNotExistException(catalogName(), tablePath, e);
    } catch (Exception e) {
      throw new CatalogException(e);
    }
  }

  @Override
  public void alterTable(ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists)
      throws TableNotExistException, CatalogException {
    Table table = loadGravitinoTable(tablePath, ignoreIfNotExists);
    if (table == null) {
      return;
    }
    if (!FlinkGenericTableUtil.isGenericTableWhenLoad(table.properties())) {
      super.alterTable(tablePath, newTable, ignoreIfNotExists);
      return;
    }
    if (!(newTable instanceof ResolvedCatalogTable)) {
      throw new CatalogException("Generic table must be a resolved catalog table");
    }
    // For generic tables, we re-serialize the entire table schema and partition keys into
    // flink.* properties, so the individual tableChanges are not needed. The newTable
    // parameter contains the final state after applying all changes.
    applyGenericTableAlter(tablePath, table, (ResolvedCatalogTable) newTable);
  }

  @Override
  public void alterTable(
      ObjectPath tablePath,
      CatalogBaseTable newTable,
      java.util.List<org.apache.flink.table.catalog.TableChange> tableChanges,
      boolean ignoreIfNotExists)
      throws TableNotExistException, CatalogException {
    Table table = loadGravitinoTable(tablePath, ignoreIfNotExists);
    if (table == null) {
      return;
    }
    if (!FlinkGenericTableUtil.isGenericTableWhenLoad(table.properties())) {
      super.alterTable(tablePath, newTable, tableChanges, ignoreIfNotExists);
      return;
    }
    if (!(newTable instanceof ResolvedCatalogTable)) {
      throw new CatalogException("Generic table must be a resolved catalog table");
    }
    applyGenericTableAlter(tablePath, table, (ResolvedCatalogTable) newTable);
  }

  private Table loadGravitinoTable(ObjectPath tablePath, boolean ignoreIfNotExists)
      throws TableNotExistException, CatalogException {
    try {
      return catalog()
          .asTableCatalog()
          .loadTable(NameIdentifier.of(tablePath.getDatabaseName(), tablePath.getObjectName()));
    } catch (NoSuchTableException e) {
      if (!ignoreIfNotExists) {
        throw new TableNotExistException(catalogName(), tablePath, e);
      }
      return null;
    } catch (Exception e) {
      throw new CatalogException(e);
    }
  }

  private void applyGenericTableAlter(
      ObjectPath tablePath, Table existingTable, ResolvedCatalogTable newTable)
      throws TableNotExistException, CatalogException {
    NameIdentifier identifier =
        NameIdentifier.of(tablePath.getDatabaseName(), tablePath.getObjectName());
    Map<String, String> updatedProperties =
        FlinkGenericTableUtil.toGravitinoGenericTableProperties(newTable);
    Map<String, String> currentProperties =
        existingTable.properties() == null ? Collections.emptyMap() : existingTable.properties();

    List<TableChange> changes = new ArrayList<>();
    if (!Objects.equals(existingTable.comment(), newTable.getComment())) {
      changes.add(TableChange.updateComment(newTable.getComment()));
    }

    currentProperties.keySet().stream()
        .filter(
            key ->
                (key.startsWith(CatalogPropertiesUtil.FLINK_PROPERTY_PREFIX)
                        || CatalogPropertiesUtil.IS_GENERIC.equals(key))
                    && !updatedProperties.containsKey(key))
        .forEach(key -> changes.add(TableChange.removeProperty(key)));

    updatedProperties.forEach(
        (key, value) -> {
          String currentValue = currentProperties.get(key);
          if (!value.equals(currentValue)) {
            changes.add(TableChange.setProperty(key, value));
          }
        });

    try {
      catalog().asTableCatalog().alterTable(identifier, changes.toArray(new TableChange[0]));
    } catch (NoSuchTableException e) {
      throw new TableNotExistException(catalogName(), tablePath, e);
    } catch (Exception e) {
      throw new CatalogException(e);
    }
  }
}
