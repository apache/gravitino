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
package org.apache.gravitino.catalog.jdbc;

import static org.apache.gravitino.connector.BaseCatalog.CATALOG_BYPASS_PREFIX;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.sql.DataSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.StringIdentifier;
import org.apache.gravitino.catalog.jdbc.config.JdbcConfig;
import org.apache.gravitino.catalog.jdbc.converter.JdbcColumnDefaultValueConverter;
import org.apache.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import org.apache.gravitino.catalog.jdbc.operation.DatabaseOperation;
import org.apache.gravitino.catalog.jdbc.operation.JdbcDatabaseOperations;
import org.apache.gravitino.catalog.jdbc.operation.JdbcTableOperations;
import org.apache.gravitino.catalog.jdbc.operation.RequireDatabaseOperation;
import org.apache.gravitino.catalog.jdbc.operation.TableOperation;
import org.apache.gravitino.catalog.jdbc.utils.DataSourceUtils;
import org.apache.gravitino.connector.CatalogInfo;
import org.apache.gravitino.connector.CatalogOperations;
import org.apache.gravitino.connector.HasPropertyMetadata;
import org.apache.gravitino.connector.SupportsSchemas;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.NonEmptySchemaException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.utils.IsolatedClassLoader;
import org.apache.gravitino.utils.MapUtils;
import org.apache.gravitino.utils.PrincipalUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Operations for interacting with the Jdbc catalog in Apache Gravitino. */
public class JdbcCatalogOperations implements CatalogOperations, SupportsSchemas, TableCatalog {

  private static final String GRAVITINO_ATTRIBUTE_DOES_NOT_EXIST_MSG =
      "The Gravitino id attribute does not exist in properties";

  public static final Logger LOG = LoggerFactory.getLogger(JdbcCatalogOperations.class);

  private JdbcCatalogPropertiesMetadata jdbcCatalogPropertiesMetadata;

  private JdbcTablePropertiesMetadata jdbcTablePropertiesMetadata;

  private final JdbcExceptionConverter exceptionConverter;

  private final JdbcTypeConverter jdbcTypeConverter;

  private final DatabaseOperation databaseOperation;

  private final TableOperation tableOperation;

  private DataSource dataSource;

  private final JdbcColumnDefaultValueConverter columnDefaultValueConverter;

  public static class JDBCDriverInfo {
    public String name;
    public String version;
    public int majorVersion;
    public int minorVersion;

    public JDBCDriverInfo(String driverName, String version, int majorVersion, int minorVersion) {
      this.name = driverName;
      this.version = version;
      this.majorVersion = majorVersion;
      this.minorVersion = minorVersion;
    }
  }

  /**
   * Constructs a new instance of JdbcCatalogOperations.
   *
   * @param exceptionConverter The exception converter to be used by the operations.
   * @param jdbcTypeConverter The type converter to be used by the operations.
   * @param databaseOperation The database operations to be used by the operations.
   * @param tableOperation The table operations to be used by the operations.
   * @param columnDefaultValueConverter The column default value converter to be used by the
   *     operations.
   */
  public JdbcCatalogOperations(
      JdbcExceptionConverter exceptionConverter,
      JdbcTypeConverter jdbcTypeConverter,
      JdbcDatabaseOperations databaseOperation,
      JdbcTableOperations tableOperation,
      JdbcColumnDefaultValueConverter columnDefaultValueConverter) {
    this.exceptionConverter = exceptionConverter;
    this.jdbcTypeConverter = jdbcTypeConverter;
    this.databaseOperation = databaseOperation;
    this.tableOperation = tableOperation;
    this.columnDefaultValueConverter = columnDefaultValueConverter;
  }

  /**
   * Initializes the Jdbc catalog operations with the provided configuration.
   *
   * @param conf The configuration map for the Jdbc catalog operations.
   * @param info The catalog info associated with this operations instance.
   * @throws RuntimeException if initialization fails.
   */
  @Override
  public void initialize(
      Map<String, String> conf, CatalogInfo info, HasPropertyMetadata propertiesMetadata)
      throws RuntimeException {
    this.jdbcCatalogPropertiesMetadata =
        (JdbcCatalogPropertiesMetadata) propertiesMetadata.catalogPropertiesMetadata();
    this.jdbcTablePropertiesMetadata =
        (JdbcTablePropertiesMetadata) propertiesMetadata.tablePropertiesMetadata();

    // Key format like gravitino.bypass.a.b
    Map<String, String> prefixMap = MapUtils.getPrefixMap(conf, CATALOG_BYPASS_PREFIX);

    // Hold keys that lie in JDBC_IMMUTABLE_PROPERTIES
    Map<String, String> gravitinoConfig =
        this.jdbcCatalogPropertiesMetadata.transformProperties(conf);
    Map<String, String> resultConf = Maps.newHashMap(prefixMap);
    resultConf.putAll(gravitinoConfig);

    JdbcConfig jdbcConfig = new JdbcConfig(resultConf);
    this.dataSource = DataSourceUtils.createDataSource(jdbcConfig);

    checkJDBCDriverVersion();
    this.databaseOperation.initialize(dataSource, exceptionConverter, resultConf);
    this.tableOperation.initialize(
        dataSource, exceptionConverter, jdbcTypeConverter, columnDefaultValueConverter, resultConf);
    if (tableOperation instanceof RequireDatabaseOperation) {
      ((RequireDatabaseOperation) tableOperation).setDatabaseOperation(databaseOperation);
    }
  }

  /** Closes the Jdbc catalog and releases the associated client pool. */
  @Override
  public void close() {
    DataSourceUtils.closeDataSource(dataSource);
  }

  /**
   * Lists the schemas under the given namespace.
   *
   * @param namespace The namespace to list the schemas for.
   * @return An array of {@link NameIdentifier} representing the schemas.
   * @throws NoSuchCatalogException If the provided namespace is invalid or does not exist.
   */
  @Override
  public NameIdentifier[] listSchemas(Namespace namespace) throws NoSuchCatalogException {
    List<String> schemaNames = databaseOperation.listDatabases();
    return schemaNames.stream()
        .map(db -> NameIdentifier.of(namespace, db))
        .toArray(NameIdentifier[]::new);
  }

  /**
   * Performs `show databases` operation to check if the JDBC connection is valid.
   *
   * @param catalogIdent the name of the catalog.
   * @param type the type of the catalog.
   * @param provider the provider of the catalog.
   * @param comment the comment of the catalog.
   * @param properties the properties of the catalog.
   */
  @Override
  public void testConnection(
      NameIdentifier catalogIdent,
      Catalog.Type type,
      String provider,
      String comment,
      Map<String, String> properties) {
    databaseOperation.listDatabases();
  }

  /**
   * Creates a new schema with the provided identifier, comment and metadata.
   *
   * @param ident The identifier of the schema to create.
   * @param comment The comment for the schema.
   * @param properties The properties for the schema.
   * @return The created {@link JdbcSchema}.
   * @throws NoSuchCatalogException If the provided namespace is invalid or does not exist.
   * @throws SchemaAlreadyExistsException If a schema with the same name already exists.
   */
  @Override
  public JdbcSchema createSchema(
      NameIdentifier ident, String comment, Map<String, String> properties)
      throws NoSuchCatalogException, SchemaAlreadyExistsException {
    StringIdentifier identifier =
        Preconditions.checkNotNull(
            StringIdentifier.fromProperties(properties), GRAVITINO_ATTRIBUTE_DOES_NOT_EXIST_MSG);
    String notAllowedKey =
        properties.keySet().stream()
            .filter(s -> !StringUtils.equals(s, StringIdentifier.ID_KEY))
            .collect(Collectors.joining(","));
    if (StringUtils.isNotEmpty(notAllowedKey)) {
      LOG.warn("The properties [{}] are not allowed to be set in the jdbc schema", notAllowedKey);
    }
    HashMap<String, String> resultProperties = Maps.newHashMap(properties);
    resultProperties.remove(StringIdentifier.ID_KEY);
    databaseOperation.create(
        ident.name(), StringIdentifier.addToComment(identifier, comment), resultProperties);
    return JdbcSchema.builder()
        .withName(ident.name())
        .withProperties(resultProperties)
        .withComment(comment)
        .withAuditInfo(
            AuditInfo.builder().withCreator(currentUser()).withCreateTime(Instant.now()).build())
        .build();
  }

  /**
   * Loads the schema with the provided identifier.
   *
   * @param ident The identifier of the schema to load.
   * @return The loaded {@link JdbcSchema}.
   * @throws NoSuchSchemaException If the schema with the provided identifier does not exist.
   */
  @Override
  public JdbcSchema loadSchema(NameIdentifier ident) throws NoSuchSchemaException {
    JdbcSchema load = databaseOperation.load(ident.name());
    String comment = load.comment();
    StringIdentifier id = StringIdentifier.fromComment(comment);
    if (id == null) {
      LOG.warn("The comment {} does not contain Gravitino id attribute", comment);
      return load;
    }
    Map<String, String> properties =
        load.properties() == null ? Maps.newHashMap() : Maps.newHashMap(load.properties());
    return JdbcSchema.builder()
        .withAuditInfo(load.auditInfo())
        .withName(load.name())
        .withComment(StringIdentifier.removeIdFromComment(load.comment()))
        .withProperties(StringIdentifier.newPropertiesWithId(id, properties))
        .build();
  }

  /**
   * Alters the schema with the provided identifier according to the specified changes.
   *
   * @param ident The identifier of the schema to alter.
   * @param changes The changes to apply to the schema.
   * @return The altered {@link JdbcSchema}.
   * @throws NoSuchSchemaException If the schema with the provided identifier does not exist.
   */
  @Override
  public JdbcSchema alterSchema(NameIdentifier ident, SchemaChange... changes)
      throws NoSuchSchemaException {
    throw new UnsupportedOperationException("jdbc-catalog does not support alter the schema");
  }

  /**
   * Drops the schema with the provided identifier.
   *
   * @param ident The identifier of the schema to drop.
   * @param cascade If set to true, drops all the tables in the schema as well.
   * @return true if the schema is successfully dropped; false if the schema does not exist.
   * @throws NonEmptySchemaException If the schema is not empty and 'cascade' is set to false.
   */
  @Override
  public boolean dropSchema(NameIdentifier ident, boolean cascade) throws NonEmptySchemaException {
    return databaseOperation.delete(ident.name(), cascade);
  }

  /**
   * Lists all the tables under the specified namespace.
   *
   * @param namespace The namespace to list tables for.
   * @return An array of {@link NameIdentifier} representing the tables in the namespace.
   * @throws NoSuchSchemaException If the schema with the provided namespace does not exist.
   */
  @Override
  public NameIdentifier[] listTables(Namespace namespace) throws NoSuchSchemaException {
    String databaseName = NameIdentifier.of(namespace.levels()).name();
    return tableOperation.listTables(databaseName).stream()
        .map(table -> NameIdentifier.of(namespace, table))
        .toArray(NameIdentifier[]::new);
  }

  /**
   * Loads a table from the Jdbc.
   *
   * @param tableIdent The identifier of the table to load.
   * @return The loaded JdbcTable instance representing the table.
   * @throws NoSuchTableException If the specified table does not exist in the Jdbc.
   */
  @Override
  public Table loadTable(NameIdentifier tableIdent) throws NoSuchTableException {
    String databaseName = NameIdentifier.of(tableIdent.namespace().levels()).name();
    String tableName = tableIdent.name();
    JdbcTable load = tableOperation.load(databaseName, tableName);
    Map<String, String> properties =
        load.properties() == null
            ? Maps.newHashMap()
            : jdbcTablePropertiesMetadata.convertFromJdbcProperties(load.properties());
    String comment = load.comment();
    StringIdentifier id = StringIdentifier.fromComment(comment);
    if (id == null) {
      LOG.warn(
          "The table {} comment {} does not contain Gravitino id attribute", tableName, comment);
    } else {
      properties = StringIdentifier.newPropertiesWithId(id, properties);
      // Remove id from comment
      comment = StringIdentifier.removeIdFromComment(comment);
    }
    return JdbcTable.builder()
        .withAuditInfo(load.auditInfo())
        .withName(tableName)
        .withColumns(load.columns())
        .withAuditInfo(load.auditInfo())
        .withComment(comment)
        .withProperties(properties)
        .withDistribution(load.distribution())
        .withIndexes(load.index())
        .withPartitioning(load.partitioning())
        .withDatabaseName(databaseName)
        .withTableOperation(tableOperation)
        .build();
  }

  /**
   * Apply the {@link TableChange change} to an existing Jdbc table.
   *
   * @param tableIdent The identifier of the table to alter.
   * @param changes The changes to apply to the table.
   * @return The altered JdbcTable instance representing the table.
   * @throws NoSuchTableException This exception will not be thrown in this method.
   * @throws IllegalArgumentException This exception will not be thrown in this method.
   */
  @Override
  public Table alterTable(NameIdentifier tableIdent, TableChange... changes)
      throws NoSuchTableException, IllegalArgumentException {
    Optional<TableChange> renameTableOptional =
        Arrays.stream(changes)
            .filter(tableChange -> tableChange instanceof TableChange.RenameTable)
            .reduce((a, b) -> b);
    if (renameTableOptional.isPresent()) {
      String otherChange =
          Arrays.stream(changes)
              .filter(tableChange -> !(tableChange instanceof TableChange.RenameTable))
              .map(String::valueOf)
              .collect(Collectors.joining("\n"));
      Preconditions.checkArgument(
          StringUtils.isEmpty(otherChange),
          String.format(
              "The operation to change the table name cannot be performed together with other operations."
                  + "The list of operations that you cannot perform includes: \n %s",
              otherChange));
      return renameTable(tableIdent, (TableChange.RenameTable) renameTableOptional.get());
    }
    return internalAlterTable(tableIdent, changes);
  }

  /**
   * Drops a table from the Jdbc.
   *
   * @param tableIdent The identifier of the table to drop.
   * @return true if the table is successfully dropped; false if the table does not exist.
   */
  @Override
  public boolean dropTable(NameIdentifier tableIdent) {
    String databaseName = NameIdentifier.of(tableIdent.namespace().levels()).name();
    return tableOperation.drop(databaseName, tableIdent.name());
  }

  /**
   * Creates a new table in the Jdbc.
   *
   * @param tableIdent The identifier of the table to create.
   * @param columns The array of columns for the new table.
   * @param comment The comment for the new table.
   * @param properties The properties for the new table.
   * @param partitioning The partitioning for the new table.
   * @param indexes The indexes for the new table.
   * @return The newly created JdbcTable instance.
   * @throws NoSuchSchemaException If the schema for the table does not exist.
   * @throws TableAlreadyExistsException If the table with the same name already exists.
   */
  @Override
  public Table createTable(
      NameIdentifier tableIdent,
      Column[] columns,
      String comment,
      Map<String, String> properties,
      Transform[] partitioning,
      Distribution distribution,
      SortOrder[] sortOrders,
      Index[] indexes)
      throws NoSuchSchemaException, TableAlreadyExistsException {
    Preconditions.checkArgument(
        null == sortOrders || sortOrders.length == 0, "jdbc-catalog does not support sort orders");

    StringIdentifier identifier =
        Preconditions.checkNotNull(
            StringIdentifier.fromProperties(properties), GRAVITINO_ATTRIBUTE_DOES_NOT_EXIST_MSG);
    // The properties we write to the database do not require the id field, so it needs to be
    // removed.
    HashMap<String, String> resultProperties =
        Maps.newHashMap(jdbcTablePropertiesMetadata.transformToJdbcProperties(properties));
    JdbcColumn[] jdbcColumns =
        Arrays.stream(columns)
            .map(
                column ->
                    JdbcColumn.builder()
                        .withName(column.name())
                        .withType(column.dataType())
                        .withComment(column.comment())
                        .withNullable(column.nullable())
                        .withAutoIncrement(column.autoIncrement())
                        .withDefaultValue(column.defaultValue())
                        .withAuditInfo(AuditInfo.EMPTY)
                        .build())
            .toArray(JdbcColumn[]::new);
    String databaseName = NameIdentifier.of(tableIdent.namespace().levels()).name();
    String tableName = tableIdent.name();

    tableOperation.create(
        databaseName,
        tableName,
        jdbcColumns,
        StringIdentifier.addToComment(identifier, comment),
        resultProperties,
        partitioning,
        distribution,
        indexes);

    return JdbcTable.builder()
        .withAuditInfo(
            AuditInfo.builder().withCreator(currentUser()).withCreateTime(Instant.now()).build())
        .withName(tableName)
        .withColumns(columns)
        .withComment(comment)
        .withProperties(jdbcTablePropertiesMetadata.convertFromJdbcProperties(resultProperties))
        .withPartitioning(partitioning)
        .withIndexes(indexes)
        .withDatabaseName(databaseName)
        .withTableOperation(tableOperation)
        .build();
  }

  /**
   * Purges a table from the Jdbc.
   *
   * @param tableIdent The identifier of the table to purge.
   * @return true if the table is successfully purged; false if the table does not exist.
   * @throws UnsupportedOperationException If the table type is EXTERNAL_TABLE, it cannot be purged.
   */
  @Override
  public boolean purgeTable(NameIdentifier tableIdent) throws UnsupportedOperationException {
    String databaseName = NameIdentifier.of(tableIdent.namespace().levels()).name();
    return tableOperation.purge(databaseName, tableIdent.name());
  }

  /**
   * Perform name change operations on the Jdbc.
   *
   * @param tableIdent tableIdent of this table.
   * @param renameTable Table Change to modify the table name.
   * @return Returns the table for Iceberg.
   * @throws NoSuchTableException
   * @throws IllegalArgumentException
   */
  private Table renameTable(NameIdentifier tableIdent, TableChange.RenameTable renameTable)
      throws NoSuchTableException, IllegalArgumentException {
    String databaseName = NameIdentifier.of(tableIdent.namespace().levels()).name();
    tableOperation.rename(databaseName, tableIdent.name(), renameTable.getNewName());
    return loadTable(NameIdentifier.of(tableIdent.namespace(), renameTable.getNewName()));
  }

  /**
   * Get the JDBC driver name and version
   *
   * @return Returns the JDBC driver info
   */
  public JDBCDriverInfo getDiverInfo() {
    try (Connection conn = dataSource.getConnection()) {
      DatabaseMetaData metaData = conn.getMetaData();
      return new JDBCDriverInfo(
          metaData.getDriverName(),
          metaData.getDriverVersion(),
          metaData.getDriverMajorVersion(),
          metaData.getDriverMinorVersion());
    } catch (final SQLException se) {
      throw exceptionConverter.toGravitinoException(se);
    }
  }

  /** Check if the JDBC driver version is supported. If not, throw an exception. */
  public void checkJDBCDriverVersion() {}

  private Table internalAlterTable(NameIdentifier tableIdent, TableChange... changes)
      throws NoSuchTableException, IllegalArgumentException {
    String databaseName = NameIdentifier.of(tableIdent.namespace().levels()).name();
    TableChange[] resultChanges = replaceJdbcProperties(changes);
    tableOperation.alterTable(databaseName, tableIdent.name(), resultChanges);
    return loadTable(tableIdent);
  }

  private TableChange[] replaceJdbcProperties(TableChange[] changes) {
    // Replace jdbc properties
    return Arrays.stream(changes)
        .flatMap(
            tableChange -> {
              if (tableChange instanceof TableChange.SetProperty) {
                TableChange.SetProperty setProperty = (TableChange.SetProperty) tableChange;
                Map<String, String> jdbcProperties =
                    jdbcTablePropertiesMetadata.transformToJdbcProperties(
                        Collections.singletonMap(
                            setProperty.getProperty(), setProperty.getValue()));
                return jdbcProperties.entrySet().stream()
                    .map(entry -> TableChange.setProperty(entry.getKey(), entry.getValue()));
              } else if (tableChange instanceof TableChange.RemoveProperty) {
                TableChange.RemoveProperty removeProperty =
                    (TableChange.RemoveProperty) tableChange;
                Map<String, String> jdbcProperties =
                    jdbcTablePropertiesMetadata.transformToJdbcProperties(
                        Collections.singletonMap(removeProperty.getProperty(), null));
                return jdbcProperties.keySet().stream().map(TableChange::removeProperty);
              } else {
                return Stream.of(tableChange);
              }
            })
        .toArray(TableChange[]::new);
  }

  private static String currentUser() {
    return PrincipalUtils.getCurrentUserName();
  }

  public void deregisterDriver(Driver driver) throws SQLException {
    if (driver.getClass().getClassLoader().getClass()
        == IsolatedClassLoader.CUSTOM_CLASS_LOADER_CLASS) {
      DriverManager.deregisterDriver(driver);
      LOG.info("Driver {} has been deregistered...", driver);
    }
  }
}
