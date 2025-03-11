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
package org.apache.gravitino.catalog.lakehouse.hudi.backend.hms;

import static org.apache.gravitino.catalog.lakehouse.hudi.HudiCatalogPropertiesMetadata.URI;
import static org.apache.gravitino.connector.BaseCatalog.CATALOG_BYPASS_PREFIX;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.catalog.lakehouse.hudi.HudiSchema;
import org.apache.gravitino.catalog.lakehouse.hudi.HudiTable;
import org.apache.gravitino.catalog.lakehouse.hudi.ops.HudiCatalogBackendOps;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.NonEmptySchemaException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;
import org.apache.gravitino.hive.CachedClientPool;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.thrift.TException;

public class HudiHMSBackendOps implements HudiCatalogBackendOps {

  // Mapping from Gravitino config to Hive config
  private static final Map<String, String> CONFIG_CONVERTER =
      ImmutableMap.of(URI, HiveConf.ConfVars.METASTOREURIS.varname);

  private static final String HUDI_PACKAGE_PREFIX = "org.apache.hudi";

  @VisibleForTesting CachedClientPool clientPool;

  @Override
  public void initialize(Map<String, String> properties) {
    this.clientPool = new CachedClientPool(buildHiveConf(properties), properties);
  }

  @Override
  public HudiSchema loadSchema(NameIdentifier schemaIdent) throws NoSuchSchemaException {
    try {
      Database database = clientPool.run(client -> client.getDatabase(schemaIdent.name()));
      return HudiHMSSchema.builder().withBackendSchema(database).build();

    } catch (NoSuchObjectException | UnknownDBException e) {
      throw new NoSuchSchemaException(
          e, "Hudi schema (database) does not exist: %s in Hive Metastore", schemaIdent.name());

    } catch (TException e) {
      throw new RuntimeException(
          "Failed to load Hudi schema (database) " + schemaIdent.name() + " from Hive Metastore",
          e);

    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public NameIdentifier[] listSchemas(Namespace namespace) throws NoSuchCatalogException {
    try {
      return clientPool.run(
          c ->
              c.getAllDatabases().stream()
                  .map(db -> NameIdentifier.of(namespace, db))
                  .toArray(NameIdentifier[]::new));

    } catch (TException e) {
      throw new RuntimeException(
          "Failed to list all schemas (database) under namespace : "
              + namespace
              + " in Hive Metastore",
          e);

    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public HudiHMSSchema createSchema(
      NameIdentifier ident, String comment, Map<String, String> properties)
      throws NoSuchCatalogException, SchemaAlreadyExistsException {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public HudiHMSSchema alterSchema(NameIdentifier ident, SchemaChange... changes)
      throws NoSuchSchemaException {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public boolean dropSchema(NameIdentifier ident, boolean cascade) throws NonEmptySchemaException {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public NameIdentifier[] listTables(Namespace namespace) throws NoSuchSchemaException {
    NameIdentifier schemaIdent = NameIdentifier.of(namespace.levels());
    if (!schemaExists(schemaIdent)) {
      throw new NoSuchSchemaException("Schema (database) does not exist %s", namespace);
    }

    try {
      return clientPool.run(
          c -> {
            List<String> allTables = c.getAllTables(schemaIdent.name());
            return c.getTableObjectsByName(schemaIdent.name(), allTables).stream()
                .filter(this::checkHudiTable)
                .map(t -> NameIdentifier.of(namespace, t.getTableName()))
                .toArray(NameIdentifier[]::new);
          });

    } catch (UnknownDBException e) {
      throw new NoSuchSchemaException(
          "Schema (database) does not exist %s in Hive Metastore", namespace);

    } catch (TException e) {
      throw new RuntimeException(
          "Failed to list all tables under the namespace : " + namespace + " in Hive Metastore", e);

    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public HudiTable loadTable(NameIdentifier tableIdent) throws NoSuchTableException {
    NameIdentifier schemaIdent = NameIdentifier.of(tableIdent.namespace().levels());

    try {
      Table table =
          clientPool.run(client -> client.getTable(schemaIdent.name(), tableIdent.name()));
      if (!checkHudiTable(table)) {
        throw new NoSuchTableException(
            "Table %s is not a Hudi table in Hive Metastore", tableIdent.name());
      }
      return HudiHMSTable.builder().withBackendTable(table).build();

    } catch (NoSuchObjectException e) {
      throw new NoSuchTableException(
          e, "Hudi table does not exist: %s in Hive Metastore", tableIdent.name());

    } catch (TException e) {
      throw new RuntimeException(
          "Failed to load Hudi table " + tableIdent.name() + " from Hive metastore", e);

    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public HudiHMSTable createTable(
      NameIdentifier ident,
      Column[] columns,
      String comment,
      Map<String, String> properties,
      Transform[] partitions,
      Distribution distribution,
      SortOrder[] sortOrders,
      Index[] indexes)
      throws NoSuchSchemaException, TableAlreadyExistsException {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public HudiHMSTable alterTable(NameIdentifier ident, TableChange... changes)
      throws NoSuchTableException, IllegalArgumentException {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public boolean dropTable(NameIdentifier ident) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public void close() {
    if (clientPool != null) {
      clientPool.close();
      clientPool = null;
    }
  }

  private boolean checkHudiTable(Table table) {
    // here uses the input format to filter out non-Hudi tables, the COW table
    // uses `org.apache.hudi.hadoop.HoodieParquetInputFormat` and MOR table
    // uses `org.apache.hudi.hadoop.HoodieParquetRealtimeInputFormat`, to
    // simplify the logic, we just check the prefix of the input format
    return table.getSd().getInputFormat() != null
        && table.getSd().getInputFormat().startsWith(HUDI_PACKAGE_PREFIX);
  }

  private HiveConf buildHiveConf(Map<String, String> properties) {
    Configuration hadoopConf = new Configuration();

    Map<String, String> byPassConfigs = Maps.newHashMap();
    Map<String, String> convertedConfigs = Maps.newHashMap();
    properties.forEach(
        (key, value) -> {
          if (key.startsWith(CATALOG_BYPASS_PREFIX)) {
            byPassConfigs.put(key.substring(CATALOG_BYPASS_PREFIX.length()), value);
          } else if (CONFIG_CONVERTER.containsKey(key)) {
            convertedConfigs.put(CONFIG_CONVERTER.get(key), value);
          }
        });
    byPassConfigs.forEach(hadoopConf::set);
    // Gravitino conf has higher priority than bypass conf
    convertedConfigs.forEach(hadoopConf::set);

    return new HiveConf(hadoopConf, HudiHMSBackendOps.class);
  }
}
