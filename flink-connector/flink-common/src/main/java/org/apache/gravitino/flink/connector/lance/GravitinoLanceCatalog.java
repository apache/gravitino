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

package org.apache.gravitino.flink.connector.lance;

import java.lang.reflect.Constructor;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogColumnStatistics;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTableStatistics;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotEmptyException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.FunctionAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionAlreadyExistsException;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionSpecInvalidException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotPartitionedException;
import org.apache.flink.table.catalog.exceptions.TablePartitionedException;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.util.Preconditions;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.flink.connector.PartitionConverter;
import org.apache.gravitino.flink.connector.SchemaAndTablePropertiesConverter;
import org.apache.gravitino.flink.connector.catalog.BaseCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Gravitino Lance Catalog implementation for Apache Flink.
 *
 * <p>All schema and table DDL operations route through Apache Gravitino server, keeping Gravitino
 * as the authoritative source of truth. The catalog enables accessing tables without requiring
 * manually specified storage paths for every individual table in Flink SQL.
 */
public abstract class GravitinoLanceCatalog extends BaseCatalog {

  private static final Logger LOG = LoggerFactory.getLogger(GravitinoLanceCatalog.class);

  private final Map<String, String> catalogOptions;
  private AbstractCatalog lanceCatalog;

  /**
   * Constructs a new {@link GravitinoLanceCatalog}.
   *
   * @param catalogName the catalog name
   * @param defaultDatabase the default database name
   * @param schemaAndTablePropertiesConverter the properties converter
   * @param partitionConverter the partition converter
   * @param catalogOptions the catalog configuration options
   */
  protected GravitinoLanceCatalog(
      String catalogName,
      String defaultDatabase,
      SchemaAndTablePropertiesConverter schemaAndTablePropertiesConverter,
      PartitionConverter partitionConverter,
      Map<String, String> catalogOptions) {
    this(
        catalogName,
        defaultDatabase,
        schemaAndTablePropertiesConverter,
        partitionConverter,
        catalogOptions,
        null);
  }

  /**
   * Constructs a new {@link GravitinoLanceCatalog} with an explicit inner catalog.
   *
   * @param catalogName the catalog name
   * @param defaultDatabase the default database name
   * @param schemaAndTablePropertiesConverter the properties converter
   * @param partitionConverter the partition converter
   * @param catalogOptions the catalog configuration options
   * @param innerCatalog an explicit inner catalog instance
   */
  protected GravitinoLanceCatalog(
      String catalogName,
      String defaultDatabase,
      SchemaAndTablePropertiesConverter schemaAndTablePropertiesConverter,
      PartitionConverter partitionConverter,
      Map<String, String> catalogOptions,
      AbstractCatalog innerCatalog) {
    super(
        catalogName,
        catalogOptions,
        defaultDatabase,
        schemaAndTablePropertiesConverter,
        partitionConverter);
    this.catalogOptions = catalogOptions;
    this.lanceCatalog = innerCatalog;
  }

  @Override
  public void open() throws CatalogException {
    if (lanceCatalog == null) {
      String uri = getUri();
      Preconditions.checkArgument(
          StringUtils.isNotBlank(uri),
          "Lance REST catalog '%s' requires a non-blank 'uri' property",
          catalogName());
      this.lanceCatalog = createInnerCatalog(getName(), getDefaultDatabase(), catalogOptions);
    }
    super.open();
  }

  @Override
  public Optional<Factory> getFactory() {
    if (lanceCatalog != null) {
      return lanceCatalog.getFactory();
    }
    return Optional.empty();
  }

  @Override
  protected AbstractCatalog realCatalog() {
    Preconditions.checkState(
        lanceCatalog != null, "Catalog '%s' has not been opened", getName());
    return lanceCatalog;
  }

  /**
   * Returns the Lance REST server endpoint URI configured for this catalog.
   *
   * @return the Lance REST server URI
   */
  public String getUri() {
    String uri = catalogOptions.get(LancePropertiesConstants.GRAVITINO_LANCE_CATALOG_URI);
    if (StringUtils.isNotBlank(uri)) {
      return uri;
    }
    uri = catalogOptions.get("lance.uri");
    if (StringUtils.isNotBlank(uri)) {
      return uri;
    }
    try {
      Catalog gravitinoCatalog = catalog();
      if (gravitinoCatalog != null && gravitinoCatalog.properties() != null) {
        uri =
            gravitinoCatalog.properties().get(LancePropertiesConstants.GRAVITINO_LANCE_CATALOG_URI);
        if (StringUtils.isNotBlank(uri)) {
          return uri;
        }
        return gravitinoCatalog.properties().get("lance.uri");
      }
    } catch (Exception e) {
      LOG.debug("Unable to load catalog info from Gravitino server during URI lookup", e);
    }
    return uri;
  }

  /**
   * Creates the inner Lance catalog delegate.
   *
   * @param catalogName the catalog name
   * @param defaultDatabase the default database name
   * @param properties the catalog properties
   * @return the created inner catalog
   */
  protected AbstractCatalog createInnerCatalog(
      String catalogName, String defaultDatabase, Map<String, String> properties) {
    try {
      Class<?> clazz =
          Class.forName("org.apache.flink.connector.lance.table.LanceNamespaceCatalog");
      for (Constructor<?> ctor : clazz.getConstructors()) {
        if (ctor.getParameterCount() == 2
            && ctor.getParameterTypes()[0].equals(String.class)
            && ctor.getParameterTypes()[1].equals(String.class)) {
          return (AbstractCatalog) ctor.newInstance(catalogName, defaultDatabase);
        }
      }
    } catch (Throwable t) {
      LOG.debug(
          "Native LanceNamespaceCatalog not found or could not be loaded, using fallback inner catalog",
          t);
    }
    return new GenericLanceInnerCatalog(catalogName, defaultDatabase);
  }

  /** Minimal fallback inner catalog used when native Lance Flink connector is absent. */
  static class GenericLanceInnerCatalog extends AbstractCatalog {

    GenericLanceInnerCatalog(String name, String defaultDatabase) {
      super(name, defaultDatabase);
    }

    @Override
    public void open() throws CatalogException {}

    @Override
    public void close() throws CatalogException {}

    @Override
    public List<String> listDatabases() throws CatalogException {
      return Collections.singletonList(getDefaultDatabase());
    }

    @Override
    public CatalogDatabase getDatabase(String databaseName)
        throws DatabaseNotExistException, CatalogException {
      if (getDefaultDatabase().equalsIgnoreCase(databaseName)) {
        return new CatalogDatabaseImpl(Collections.emptyMap(), null);
      }
      throw new DatabaseNotExistException(getName(), databaseName);
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
      return getDefaultDatabase().equalsIgnoreCase(databaseName);
    }

    @Override
    public void createDatabase(String name, CatalogDatabase database, boolean ignoreIfExists)
        throws DatabaseAlreadyExistException, CatalogException {}

    @Override
    public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
        throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {}

    @Override
    public void alterDatabase(
        String name, CatalogDatabase newDatabase, boolean ignoreIfNotExists)
        throws DatabaseNotExistException, CatalogException {}

    @Override
    public List<String> listTables(String databaseName)
        throws DatabaseNotExistException, CatalogException {
      return Collections.emptyList();
    }

    @Override
    public List<String> listViews(String databaseName)
        throws DatabaseNotExistException, CatalogException {
      return Collections.emptyList();
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath)
        throws TableNotExistException, CatalogException {
      throw new TableNotExistException(getName(), tablePath);
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
      return false;
    }

    @Override
    public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists)
        throws TableNotExistException, CatalogException {}

    @Override
    public void renameTable(
        ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists)
        throws TableNotExistException, TableAlreadyExistException, CatalogException {}

    @Override
    public void createTable(
        ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists)
        throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {}

    @Override
    public void alterTable(
        ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists)
        throws TableNotExistException, CatalogException {}

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath)
        throws TableNotExistException, TableNotPartitionedException, CatalogException {
      return Collections.emptyList();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(
        ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
        throws TableNotExistException,
            TableNotPartitionedException,
            PartitionSpecInvalidException,
            CatalogException {
      return Collections.emptyList();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitionsByFilter(
        ObjectPath tablePath, List<Expression> filters)
        throws TableNotExistException, TableNotPartitionedException, CatalogException {
      return Collections.emptyList();
    }

    @Override
    public CatalogPartition getPartition(
        ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
        throws PartitionNotExistException, CatalogException {
      throw new PartitionNotExistException(getName(), tablePath, partitionSpec);
    }

    @Override
    public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
        throws CatalogException {
      return false;
    }

    @Override
    public void createPartition(
        ObjectPath tablePath,
        CatalogPartitionSpec partitionSpec,
        CatalogPartition partition,
        boolean ignoreIfExists)
        throws TableNotExistException,
            TableNotPartitionedException,
            PartitionSpecInvalidException,
            PartitionAlreadyExistsException,
            CatalogException {}

    @Override
    public void dropPartition(
        ObjectPath tablePath, CatalogPartitionSpec partitionSpec, boolean ignoreIfNotExists)
        throws TableNotExistException, PartitionNotExistException, CatalogException {}

    @Override
    public void alterPartition(
        ObjectPath tablePath,
        CatalogPartitionSpec partitionSpec,
        CatalogPartition newPartition,
        boolean ignoreIfNotExists)
        throws TableNotExistException, PartitionNotExistException, CatalogException {}

    @Override
    public List<String> listFunctions(String dbName)
        throws DatabaseNotExistException, CatalogException {
      return Collections.emptyList();
    }

    @Override
    public CatalogFunction getFunction(ObjectPath functionPath)
        throws FunctionNotExistException, CatalogException {
      throw new FunctionNotExistException(getName(), functionPath);
    }

    @Override
    public boolean functionExists(ObjectPath functionPath) throws CatalogException {
      return false;
    }

    @Override
    public void createFunction(
        ObjectPath functionPath, CatalogFunction function, boolean ignoreIfExists)
        throws FunctionAlreadyExistException, DatabaseNotExistException, CatalogException {}

    @Override
    public void alterFunction(
        ObjectPath functionPath, CatalogFunction newFunction, boolean ignoreIfNotExists)
        throws FunctionNotExistException, CatalogException {}

    @Override
    public void dropFunction(ObjectPath functionPath, boolean ignoreIfNotExists)
        throws FunctionNotExistException, CatalogException {}

    @Override
    public CatalogTableStatistics getTableStatistics(ObjectPath tablePath)
        throws TableNotExistException, CatalogException {
      return CatalogTableStatistics.UNKNOWN;
    }

    @Override
    public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath)
        throws TableNotExistException, CatalogException {
      return CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public CatalogTableStatistics getPartitionStatistics(
        ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
        throws PartitionNotExistException, CatalogException {
      return CatalogTableStatistics.UNKNOWN;
    }

    @Override
    public CatalogColumnStatistics getPartitionColumnStatistics(
        ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
        throws PartitionNotExistException, CatalogException {
      return CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public void alterTableStatistics(
        ObjectPath tablePath,
        CatalogTableStatistics tableStatistics,
        boolean ignoreIfNotExists)
        throws TableNotExistException, CatalogException {}

    @Override
    public void alterTableColumnStatistics(
        ObjectPath tablePath,
        CatalogColumnStatistics columnStatistics,
        boolean ignoreIfNotExists)
        throws TableNotExistException, CatalogException {}

    @Override
    public void alterPartitionStatistics(
        ObjectPath tablePath,
        CatalogPartitionSpec partitionSpec,
        CatalogTableStatistics partitionStatistics,
        boolean ignoreIfNotExists)
        throws PartitionNotExistException, CatalogException {}

    @Override
    public void alterPartitionColumnStatistics(
        ObjectPath tablePath,
        CatalogPartitionSpec partitionSpec,
        CatalogColumnStatistics columnStatistics,
        boolean ignoreIfNotExists)
        throws PartitionNotExistException, CatalogException {}
  }
}
