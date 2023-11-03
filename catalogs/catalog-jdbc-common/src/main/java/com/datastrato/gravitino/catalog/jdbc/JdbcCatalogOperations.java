/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.jdbc;

import static com.datastrato.gravitino.catalog.BaseCatalog.CATALOG_BYPASS_PREFIX;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.StringIdentifier;
import com.datastrato.gravitino.catalog.CatalogOperations;
import com.datastrato.gravitino.catalog.PropertiesMetadata;
import com.datastrato.gravitino.catalog.jdbc.config.JdbcConfig;
import com.datastrato.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import com.datastrato.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import com.datastrato.gravitino.catalog.jdbc.operation.JdbcDatabaseOperations;
import com.datastrato.gravitino.catalog.jdbc.utils.DataSourceUtils;
import com.datastrato.gravitino.exceptions.NoSuchCatalogException;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.exceptions.NoSuchTableException;
import com.datastrato.gravitino.exceptions.NonEmptySchemaException;
import com.datastrato.gravitino.exceptions.SchemaAlreadyExistsException;
import com.datastrato.gravitino.exceptions.TableAlreadyExistsException;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.CatalogEntity;
import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.SchemaChange;
import com.datastrato.gravitino.rel.SupportsSchemas;
import com.datastrato.gravitino.rel.Table;
import com.datastrato.gravitino.rel.TableCatalog;
import com.datastrato.gravitino.rel.TableChange;
import com.datastrato.gravitino.rel.expressions.distributions.Distribution;
import com.datastrato.gravitino.rel.expressions.sorts.SortOrder;
import com.datastrato.gravitino.rel.expressions.transforms.Transform;
import java.util.Map;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Operations for interacting with the Jdbc catalog in Gravitino. */
public class JdbcCatalogOperations implements CatalogOperations, SupportsSchemas, TableCatalog {

  public static final Logger LOG = LoggerFactory.getLogger(JdbcCatalogOperations.class);

  private JdbcCatalogPropertiesMetadata jdbcCatalogPropertiesMetadata;

  private JdbcTablePropertiesMetadata jdbcTablePropertiesMetadata;

  private JdbcSchemaPropertiesMetadata jdbcSchemaPropertiesMetadata;

  private final CatalogEntity entity;

  private final JdbcExceptionConverter exceptionConverter;

  private final JdbcTypeConverter jdbcTypeConverter;

  private final JdbcDatabaseOperations jdbcDatabaseOperations;

  private DataSource dataSource;

  /**
   * Constructs a new instance of JdbcCatalogOperations.
   *
   * @param entity The catalog entity associated with this operations instance.
   * @param exceptionConverter The exception converter to be used by the operations.
   * @param jdbcTypeConverter The type converter to be used by the operations.
   * @param jdbcDatabaseOperations The database operations to be used by the operations.
   */
  public JdbcCatalogOperations(
      CatalogEntity entity,
      JdbcExceptionConverter exceptionConverter,
      JdbcTypeConverter jdbcTypeConverter,
      JdbcDatabaseOperations jdbcDatabaseOperations) {
    this.entity = entity;
    this.exceptionConverter = exceptionConverter;
    this.jdbcTypeConverter = jdbcTypeConverter;
    this.jdbcDatabaseOperations = jdbcDatabaseOperations;
  }

  /**
   * Initializes the Jdbc catalog operations with the provided configuration.
   *
   * @param conf The configuration map for the Jdbc catalog operations.
   * @throws RuntimeException if initialization fails.
   */
  @Override
  public void initialize(Map<String, String> conf) throws RuntimeException {
    // Key format like gravitino.bypass.a.b
    Map<String, String> resultConf =
        Maps.newHashMap(MapUtils.getPrefixMap(conf, CATALOG_BYPASS_PREFIX));
    JdbcConfig jdbcConfig = new JdbcConfig(resultConf);
    this.dataSource = DataSourceUtils.createDataSource(jdbcConfig);
    this.jdbcDatabaseOperations.initialize(dataSource, exceptionConverter);
    this.jdbcCatalogPropertiesMetadata = new JdbcCatalogPropertiesMetadata();
    this.jdbcTablePropertiesMetadata = new JdbcTablePropertiesMetadata();
    this.jdbcSchemaPropertiesMetadata = new JdbcSchemaPropertiesMetadata();
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
    List<String> schemaNames = jdbcDatabaseOperations.list();
    return schemaNames.stream()
        .map(db -> NameIdentifier.of(namespace, db))
        .toArray(NameIdentifier[]::new);
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
            StringIdentifier.fromProperties(properties),
            "The gravitino id attribute does not exist in properties");
    String notAllowedKey =
        properties.keySet().stream()
            .filter(s -> !StringUtils.equals(s, StringIdentifier.ID_KEY))
            .collect(Collectors.joining(","));
    if (StringUtils.isNotEmpty(notAllowedKey)) {
      LOG.warn("The properties [{}] are not allowed to be set in the jdbc schema", notAllowedKey);
    }
    HashMap<String, String> resultProperties = Maps.newHashMap(properties);
    resultProperties.remove(StringIdentifier.ID_KEY);
    jdbcDatabaseOperations.create(
        ident.name(), StringIdentifier.addToComment(identifier, comment), resultProperties);
    return new JdbcSchema.Builder()
        .withName(ident.name())
        .withProperties(resultProperties)
        .withComment(comment)
        .withAuditInfo(AuditInfo.EMPTY)
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
    JdbcSchema load = jdbcDatabaseOperations.load(ident.name());
    String comment = load.comment();
    StringIdentifier id = StringIdentifier.fromComment(comment);
    if (id == null) {
      LOG.warn("The comment {} does not contain gravitino id attribute", comment);
      return load;
    } else {
      Map<String, String> properties =
          load.properties() == null ? Maps.newHashMap() : Maps.newHashMap(load.properties());
      StringIdentifier.addToProperties(id, properties);
      return new JdbcSchema.Builder()
          .withAuditInfo(load.auditInfo())
          .withName(load.name())
          .withComment(load.comment())
          .withProperties(properties)
          .build();
    }
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
   * @return true if the schema was dropped successfully, false otherwise.
   * @throws NonEmptySchemaException If the schema is not empty and 'cascade' is set to false.
   */
  @Override
  public boolean dropSchema(NameIdentifier ident, boolean cascade) throws NonEmptySchemaException {
    jdbcDatabaseOperations.delete(ident.name(), cascade);
    return true;
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
    throw new UnsupportedOperationException();
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
    throw new UnsupportedOperationException();
  }

  /**
   * Apply the {@link TableChange change} to an existing Jdbc table.
   *
   * @param tableIdent The identifier of the table to alter.
   * @param changes The changes to apply to the table.
   * @return This method always throws UnsupportedOperationException.
   * @throws NoSuchTableException This exception will not be thrown in this method.
   * @throws IllegalArgumentException This exception will not be thrown in this method.
   */
  @Override
  public Table alterTable(NameIdentifier tableIdent, TableChange... changes)
      throws NoSuchTableException, IllegalArgumentException {
    throw new UnsupportedOperationException();
  }

  /**
   * Drops a table from the Jdbc.
   *
   * @param tableIdent The identifier of the table to drop.
   * @return true if the table is successfully dropped; false if the table does not exist.
   */
  @Override
  public boolean dropTable(NameIdentifier tableIdent) {
    throw new UnsupportedOperationException();
  }

  /**
   * Creates a new table in the Jdbc.
   *
   * @param tableIdent The identifier of the table to create.
   * @param columns The array of columns for the new table.
   * @param comment The comment for the new table.
   * @param properties The properties for the new table.
   * @param partitioning The partitioning for the new table.
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
      SortOrder[] sortOrders)
      throws NoSuchSchemaException, TableAlreadyExistsException {
    throw new UnsupportedOperationException();
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
    throw new UnsupportedOperationException();
  }

  // TODO. We should figure out a better way to get the current user from servlet container.
  private static String currentUser() {
    return System.getProperty("user.name");
  }

  @Override
  public PropertiesMetadata tablePropertiesMetadata() throws UnsupportedOperationException {
    return jdbcTablePropertiesMetadata;
  }

  @Override
  public PropertiesMetadata catalogPropertiesMetadata() throws UnsupportedOperationException {
    return jdbcCatalogPropertiesMetadata;
  }

  @Override
  public PropertiesMetadata schemaPropertiesMetadata() throws UnsupportedOperationException {
    return jdbcSchemaPropertiesMetadata;
  }
}
