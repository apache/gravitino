/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.paimon.ops;

import static com.datastrato.gravitino.catalog.lakehouse.paimon.utils.CatalogUtils.loadCatalogBackend;
import static com.datastrato.gravitino.catalog.lakehouse.paimon.utils.TableOpsUtils.buildSchemaChanges;

import com.datastrato.gravitino.catalog.lakehouse.paimon.PaimonConfig;
import com.datastrato.gravitino.rel.TableChange;
import java.util.List;
import java.util.Map;
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
import org.apache.paimon.utils.Pair;

/** Table operation proxy that handles table operations of an underlying Paimon catalog. */
public class PaimonTableOps implements AutoCloseable {

  protected Catalog catalog;

  public PaimonTableOps(PaimonConfig paimonConfig) {
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

  public void createDatabase(Pair<String, Map<String, String>> database)
      throws DatabaseAlreadyExistException {
    catalog.createDatabase(database.getKey(), false, database.getRight());
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

  public void createTable(Pair<String, Schema> table)
      throws TableAlreadyExistException, DatabaseNotExistException {
    catalog.createTable(tableIdentifier(table.getKey()), table.getValue(), false);
  }

  public void renameTable(String fromTableName, String toTableName)
      throws TableNotExistException, TableAlreadyExistException {
    catalog.renameTable(tableIdentifier(fromTableName), tableIdentifier(toTableName), false);
  }

  public void alterTable(String tableName, TableChange... changes)
      throws TableNotExistException, ColumnAlreadyExistException, ColumnNotExistException {
    catalog.alterTable(tableIdentifier(tableName), buildSchemaChanges(changes), false);
  }

  public void dropTable(String tableName) throws TableNotExistException {
    catalog.dropTable(tableIdentifier(tableName), false);
  }

  private Identifier tableIdentifier(String tableName) {
    return Identifier.fromString(tableName);
  }
}
