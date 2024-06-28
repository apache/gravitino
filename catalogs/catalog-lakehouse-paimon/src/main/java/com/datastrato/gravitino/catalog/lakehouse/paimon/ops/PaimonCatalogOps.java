/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.paimon.ops;

import static com.datastrato.gravitino.catalog.lakehouse.paimon.utils.CatalogUtils.loadCatalogBackend;

import com.datastrato.gravitino.catalog.lakehouse.paimon.PaimonConfig;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Catalog.DatabaseAlreadyExistException;
import org.apache.paimon.catalog.Catalog.DatabaseNotEmptyException;
import org.apache.paimon.catalog.Catalog.DatabaseNotExistException;

/** Table operation proxy that handles table operations of an underlying Paimon catalog. */
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

  public void createDatabase(Pair<String, Map<String, String>> database)
      throws DatabaseAlreadyExistException {
    catalog.createDatabase(database.getKey(), false, database.getRight());
  }

  public void dropDatabase(String databaseName, boolean cascade)
      throws DatabaseNotExistException, DatabaseNotEmptyException {
    catalog.dropDatabase(databaseName, false, cascade);
  }
}
