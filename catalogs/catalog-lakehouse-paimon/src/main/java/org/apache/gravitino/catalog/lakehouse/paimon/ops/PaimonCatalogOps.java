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

import java.util.List;
import java.util.Map;
import org.apache.gravitino.catalog.lakehouse.paimon.PaimonConfig;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Catalog.DatabaseAlreadyExistException;
import org.apache.paimon.catalog.Catalog.DatabaseNotEmptyException;
import org.apache.paimon.catalog.Catalog.DatabaseNotExistException;
import org.apache.paimon.catalog.Catalog.TableNotExistException;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;

/** Table operation proxy that handles table operations of an underlying Apache Paimon catalog. */
public class PaimonCatalogOps implements AutoCloseable {

  protected Catalog catalog;

  public PaimonCatalogOps(PaimonConfig paimonConfig) {
    catalog = loadCatalogBackend(paimonConfig);
  }

  @Override
  public void close() throws Exception {
    if (catalog != null) {
      catalog.close();
    }
  }

  public List<String> listDatabases() {
    return catalog.listDatabases();
  }

  public Map<String, String> loadDatabase(String databaseName) throws DatabaseNotExistException {
    return catalog.loadDatabaseProperties(databaseName);
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

  public void dropTable(String tableName) throws TableNotExistException {
    catalog.dropTable(tableIdentifier(tableName), false);
  }

  private Identifier tableIdentifier(String tableName) {
    return Identifier.fromString(tableName);
  }
}
