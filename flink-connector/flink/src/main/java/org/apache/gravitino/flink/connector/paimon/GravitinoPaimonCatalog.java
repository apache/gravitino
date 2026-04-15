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
import java.util.Map;
import java.util.Optional;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.Factory;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.flink.connector.PartitionConverter;
import org.apache.gravitino.flink.connector.SchemaAndTablePropertiesConverter;
import org.apache.gravitino.flink.connector.catalog.BaseCatalog;
import org.apache.gravitino.rel.Table;
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
    this.paimonCatalog = flinkCatalogFactory.createCatalog(toPaimonContext(context));
  }

  /**
   * Translates Gravitino catalog property names to their Paimon/Flink equivalents so that the
   * underlying {@code FlinkCatalog} can be initialised correctly (e.g., {@code catalog-backend}
   * becomes {@code metastore}).
   */
  private static CatalogFactory.Context toPaimonContext(CatalogFactory.Context context) {
    Map<String, String> translatedOptions = new HashMap<>();
    for (Map.Entry<String, String> entry : context.getOptions().entrySet()) {
      String mappedKey =
          PaimonPropertiesConverter.INSTANCE.transformPropertyToFlinkCatalog(entry.getKey());
      // Fall back to the original key when no mapping exists so Paimon-native properties are not
      // silently dropped.
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

  /**
   * Returns the Paimon-native {@code DataCatalogTable} instead of the plain Flink {@code
   * CatalogTable} that {@link BaseCatalog#toFlinkTable} would produce from Gravitino metadata.
   *
   * <p><b>Why this matters for partition metadata:</b> Paimon's {@code
   * AbstractFlinkTableFactory.buildPaimonTable()} inspects the incoming {@code CatalogBaseTable}.
   * When it receives a plain {@code CatalogTable} (not a {@code DataCatalogTable}), it creates a
   * {@code FileStoreTable} with {@code CatalogEnvironment.empty()} — a null {@code catalogLoader}.
   * A null loader causes {@code FileStoreTable.partitionHandler()} to return {@code null}, so
   * {@code AddPartitionCommitCallback} is never registered. As a result, Hive partition metadata is
   * never updated when a Flink job commits, and the new partition does not appear in {@code SHOW
   * PARTITIONS} even though the data files exist.
   *
   * <p><b>Why this is safe:</b> Gravitino authorization has already been enforced by {@link
   * BaseCatalog#getTable(ObjectPath)}, which calls {@code catalog().asTableCatalog().loadTable()}
   * before this method is invoked. DDL operations ({@code CREATE / ALTER / DROP TABLE}) continue
   * to flow through the Gravitino REST API → Gravitino server → Paimon catalog, keeping Gravitino
   * as the single source of truth.
   */
  @Override
  protected CatalogBaseTable toFlinkTable(Table table, ObjectPath tablePath) {
    try {
      return paimonCatalog.getTable(tablePath);
    } catch (TableNotExistException e) {
      // The table was confirmed to exist in Gravitino (auth passed in BaseCatalog.getTable).
      // This branch indicates the two stores are out of sync.
      throw new CatalogException(
          "Table "
              + tablePath
              + " exists in Gravitino but not in Paimon/Hive metastore."
              + " The two stores may be out of sync.",
          e);
    }
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
  }
}
