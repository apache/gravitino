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
package org.apache.gravitino.catalog.lakehouse.paimon.ops;

import static org.apache.gravitino.catalog.lakehouse.paimon.utils.CatalogUtils.loadCatalogBackend;
import static org.apache.gravitino.catalog.lakehouse.paimon.utils.TableOpsUtils.buildSchemaChanges;

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.catalog.lakehouse.paimon.PaimonConfig;
import org.apache.gravitino.rel.TableChange;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Catalog.ColumnAlreadyExistException;
import org.apache.paimon.catalog.Catalog.ColumnNotExistException;
import org.apache.paimon.catalog.Catalog.DatabaseAlreadyExistException;
import org.apache.paimon.catalog.Catalog.DatabaseNotEmptyException;
import org.apache.paimon.catalog.Catalog.DatabaseNotExistException;
import org.apache.paimon.catalog.Catalog.TableAlreadyExistException;
import org.apache.paimon.catalog.Catalog.TableNotExistException;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;

/** Table operation proxy that handles table operations of an underlying Apache Paimon catalog. */
public class PaimonCatalogOps implements AutoCloseable {

  private final PaimonBackendCatalogWrapper paimonBackendCatalogWrapper;
  protected Catalog catalog;

  public PaimonCatalogOps(PaimonConfig paimonConfig) {
    paimonBackendCatalogWrapper = loadCatalogBackend(paimonConfig);
    Preconditions.checkArgument(
        paimonBackendCatalogWrapper.getCatalog() != null,
        "Can not load Paimon backend catalog instance.");
    catalog = paimonBackendCatalogWrapper.getCatalog();
  }

  @Override
  public void close() throws Exception {
    if (paimonBackendCatalogWrapper != null) {
      paimonBackendCatalogWrapper.close();
    }
  }

  public List<String> listDatabases() {
    return catalog.listDatabases();
  }

  public Map<String, String> loadDatabase(String databaseName) throws DatabaseNotExistException {
    return catalog.getDatabase(databaseName).options();
  }

  public void createDatabase(String databaseName, Map<String, String> properties)
      throws DatabaseAlreadyExistException {
    catalog.createDatabase(databaseName, false, properties);
  }

  public void dropDatabase(String databaseName, boolean cascade)
      throws DatabaseNotExistException, DatabaseNotEmptyException {
    catalog.dropDatabase(databaseName, false, cascade);
  }

  public List<String> listTables(String databaseName) throws DatabaseNotExistException {
    return catalog.listTables(databaseName);
  }

  public Table loadTable(String tableName) throws TableNotExistException {
    return catalog.getTable(tableIdentifier(tableName));
  }

  public void createTable(String tableName, Schema schema)
      throws Catalog.TableAlreadyExistException, DatabaseNotExistException {
    catalog.createTable(tableIdentifier(tableName), schema, false);
  }

  public void purgeTable(String tableName) throws TableNotExistException {
    catalog.dropTable(tableIdentifier(tableName), false);
  }

  public void alterTable(String tableName, TableChange... changes)
      throws ColumnAlreadyExistException, TableNotExistException, ColumnNotExistException {
    catalog.alterTable(tableIdentifier(tableName), buildSchemaChanges(changes), false);
  }

  public void renameTable(String fromTableName, String toTableName)
      throws TableNotExistException, TableAlreadyExistException {
    catalog.renameTable(tableIdentifier(fromTableName), tableIdentifier(toTableName), false);
  }

  private Identifier tableIdentifier(String tableName) {
    return Identifier.fromString(tableName);
  }
}
