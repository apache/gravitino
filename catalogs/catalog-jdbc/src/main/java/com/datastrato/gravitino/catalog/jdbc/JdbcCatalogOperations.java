/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.jdbc;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.catalog.CatalogOperations;
import com.datastrato.gravitino.catalog.PropertiesMetadata;
import com.datastrato.gravitino.exceptions.NoSuchCatalogException;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.exceptions.NoSuchTableException;
import com.datastrato.gravitino.exceptions.NonEmptySchemaException;
import com.datastrato.gravitino.exceptions.SchemaAlreadyExistsException;
import com.datastrato.gravitino.exceptions.TableAlreadyExistsException;
import com.datastrato.gravitino.meta.CatalogEntity;
import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.Distribution;
import com.datastrato.gravitino.rel.SchemaChange;
import com.datastrato.gravitino.rel.SortOrder;
import com.datastrato.gravitino.rel.SupportsSchemas;
import com.datastrato.gravitino.rel.Table;
import com.datastrato.gravitino.rel.TableCatalog;
import com.datastrato.gravitino.rel.TableChange;
import com.datastrato.gravitino.rel.transforms.Transform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/** Operations for interacting with the Jdbc catalog in Gravitino. */
public class JdbcCatalogOperations implements CatalogOperations, SupportsSchemas, TableCatalog {

  public static final Logger LOG = LoggerFactory.getLogger(JdbcCatalogOperations.class);

  private JdbcCatalogPropertiesMetadata jdbcCatalogPropertiesMetadata;

  private JdbcTablePropertiesMetadata jdbcTablePropertiesMetadata;

  private JdbcSchemaPropertiesMetadata jdbcSchemaPropertiesMetadata;

  private final CatalogEntity entity;

  /**
   * Constructs a new instance of JdbcCatalogOperations.
   *
   * @param entity The catalog entity associated with this operations instance.
   */
  public JdbcCatalogOperations(CatalogEntity entity) {
    this.entity = entity;
  }

  /**
   * Initializes the Jdbc catalog operations with the provided configuration.
   *
   * @param conf The configuration map for the Jdbc catalog operations.
   * @throws RuntimeException if initialization fails.
   */
  @Override
  public void initialize(Map<String, String> conf) throws RuntimeException {
    this.jdbcCatalogPropertiesMetadata = new JdbcCatalogPropertiesMetadata();
    this.jdbcTablePropertiesMetadata = new JdbcTablePropertiesMetadata();
    this.jdbcSchemaPropertiesMetadata = new JdbcSchemaPropertiesMetadata();
  }

  /** Closes the Jdbc catalog and releases the associated client pool. */
  @Override
  public void close() {}

  /**
   * Lists the schemas under the given namespace.
   *
   * @param namespace The namespace to list the schemas for.
   * @return An array of {@link NameIdentifier} representing the schemas.
   * @throws NoSuchCatalogException If the provided namespace is invalid or does not exist.
   */
  @Override
  public NameIdentifier[] listSchemas(Namespace namespace) throws NoSuchCatalogException {
    throw new UnsupportedOperationException();
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
    throw new UnsupportedOperationException();
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
    throw new UnsupportedOperationException();
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
    throw new UnsupportedOperationException();
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
    throw new UnsupportedOperationException();
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
   * @param partitions The partitioning for the new table.
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
      Transform[] partitions,
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
