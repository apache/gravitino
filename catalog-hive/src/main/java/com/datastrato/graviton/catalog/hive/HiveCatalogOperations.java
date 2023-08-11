/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.catalog.hive;

import static com.datastrato.graviton.catalog.hive.HiveTable.HMS_TABLE_COMMENT;
import static com.datastrato.graviton.catalog.hive.HiveTable.SUPPORT_TABLE_TYPES;

import com.datastrato.graviton.NameIdentifier;
import com.datastrato.graviton.Namespace;
import com.datastrato.graviton.catalog.CatalogOperations;
import com.datastrato.graviton.catalog.hive.converter.ToHiveType;
import com.datastrato.graviton.exceptions.NoSuchCatalogException;
import com.datastrato.graviton.exceptions.NoSuchSchemaException;
import com.datastrato.graviton.exceptions.NoSuchTableException;
import com.datastrato.graviton.exceptions.NonEmptySchemaException;
import com.datastrato.graviton.exceptions.SchemaAlreadyExistsException;
import com.datastrato.graviton.exceptions.TableAlreadyExistsException;
import com.datastrato.graviton.meta.AuditInfo;
import com.datastrato.graviton.meta.CatalogEntity;
import com.datastrato.graviton.meta.rel.BaseSchema;
import com.datastrato.graviton.meta.rel.BaseTable;
import com.datastrato.graviton.rel.Column;
import com.datastrato.graviton.rel.SchemaChange;
import com.datastrato.graviton.rel.SupportsSchemas;
import com.datastrato.graviton.rel.Table;
import com.datastrato.graviton.rel.TableCatalog;
import com.datastrato.graviton.rel.TableChange;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Operations for interacting with the Hive catalog in Graviton. */
public class HiveCatalogOperations implements CatalogOperations, SupportsSchemas, TableCatalog {

  public static final Logger LOG = LoggerFactory.getLogger(HiveCatalogOperations.class);

  @VisibleForTesting HiveClientPool clientPool;

  private HiveConf hiveConf;

  private final CatalogEntity entity;

  /**
   * Constructs a new instance of HiveCatalogOperations.
   *
   * @param entity The catalog entity associated with this operations instance.
   */
  public HiveCatalogOperations(CatalogEntity entity) {
    this.entity = entity;
  }

  /**
   * Initializes the Hive catalog operations with the provided configuration.
   *
   * @param conf The configuration map for the Hive catalog operations.
   * @throws RuntimeException if initialization fails.
   */
  @Override
  public void initialize(Map<String, String> conf) throws RuntimeException {
    Configuration hadoopConf = new Configuration();
    conf.forEach(hadoopConf::set);
    hiveConf = new HiveConf(hadoopConf, HiveCatalogOperations.class);

    // todo(xun): add hive client pool size in config
    this.clientPool = new HiveClientPool(1, hiveConf);
  }

  /** Closes the Hive catalog and releases the associated client pool. */
  @Override
  public void close() {
    if (clientPool != null) {
      clientPool.close();
      clientPool = null;
    }
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
    if (!isValidNamespace(namespace)) {
      throw new NoSuchCatalogException("Namespace is invalid " + namespace);
    }

    try {
      NameIdentifier[] schemas =
          clientPool.run(
              c ->
                  c.getAllDatabases().stream()
                      .map(db -> NameIdentifier.of(namespace, db))
                      .toArray(NameIdentifier[]::new));
      return schemas;

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

  /**
   * Creates a new schema with the provided identifier, comment, and metadata.
   *
   * @param ident The identifier of the schema to create.
   * @param comment The comment for the schema.
   * @param metadata The metadata properties for the schema.
   * @return The created {@link HiveSchema}.
   * @throws NoSuchCatalogException If the provided namespace is invalid or does not exist.
   * @throws SchemaAlreadyExistsException If a schema with the same name already exists.
   */
  @Override
  public HiveSchema createSchema(NameIdentifier ident, String comment, Map<String, String> metadata)
      throws NoSuchCatalogException, SchemaAlreadyExistsException {
    Preconditions.checkArgument(
        !ident.name().isEmpty(),
        String.format("Cannot create schema with invalid name: %s", ident.name()));
    Preconditions.checkArgument(
        isValidNamespace(ident.namespace()),
        String.format("Cannot support invalid namespace in Hive Metastore: %s", ident.namespace()));

    try {
      HiveSchema hiveSchema =
          new HiveSchema.Builder()
              .withId(1L /*TODO. Use ID generator*/)
              .withCatalogId(entity.getId())
              .withName(ident.name())
              .withNamespace(ident.namespace())
              .withComment(comment)
              .withProperties(metadata)
              .withAuditInfo(
                  new AuditInfo.Builder()
                      .withCreator(currentUser())
                      .withCreateTime(Instant.now())
                      .build())
              .withConf(hiveConf)
              .build();

      clientPool.run(
          client -> {
            client.createDatabase(hiveSchema.toInnerDB());
            return null;
          });

      // TODO. We should also store the customized HiveSchema entity fields into our own
      //  underlying storage, like id, auditInfo, etc.

      LOG.info("Created Hive schema (database) {} in Hive Metastore", ident.name());

      return hiveSchema;

    } catch (AlreadyExistsException e) {
      throw new SchemaAlreadyExistsException(
          String.format(
              "Hive schema (database) '%s' already exists in Hive Metastore", ident.name()));

    } catch (TException e) {
      throw new RuntimeException(
          "Failed to create Hive schema (database) " + ident.name() + " in Hive Metastore", e);

    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Loads the schema with the provided identifier.
   *
   * @param ident The identifier of the schema to load.
   * @return The loaded {@link HiveSchema}.
   * @throws NoSuchSchemaException If the schema with the provided identifier does not exist.
   */
  @Override
  public HiveSchema loadSchema(NameIdentifier ident) throws NoSuchSchemaException {
    Preconditions.checkArgument(
        !ident.name().isEmpty(),
        String.format("Cannot load schema with invalid name: %s", ident.name()));
    Preconditions.checkArgument(
        isValidNamespace(ident.namespace()),
        String.format("Cannot support invalid namespace in Hive Metastore: %s", ident.namespace()));

    try {
      Database database = clientPool.run(client -> client.getDatabase(ident.name()));
      HiveSchema.Builder builder = new HiveSchema.Builder();

      // TODO. We should also fetch the customized HiveSchema entity fields from our own
      //  underlying storage, like id, auditInfo, etc.

      builder =
          builder
              .withId(1L /* TODO. Fetch id from underlying storage */)
              .withCatalogId(entity.getId())
              .withNamespace(ident.namespace())
              .withAuditInfo(
                  /* TODO. Fetch audit info from underlying storage */
                  new AuditInfo.Builder()
                      .withCreator(currentUser())
                      .withCreateTime(Instant.now())
                      .build())
              .withConf(hiveConf);
      HiveSchema hiveSchema = HiveSchema.fromInnerDB(database, builder);

      LOG.info("Loaded Hive schema (database) {} from Hive Metastore ", ident.name());

      return hiveSchema;

    } catch (NoSuchObjectException | UnknownDBException e) {
      throw new NoSuchSchemaException(
          String.format(
              "Hive schema (database) does not exist: %s in Hive Metastore", ident.name()),
          e);

      // TODO. We should also delete Hive schema (database) from our own underlying storage

    } catch (TException e) {
      throw new RuntimeException(
          "Failed to load Hive schema (database) " + ident.name() + " from Hive Metastore", e);

    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Alters the schema with the provided identifier according to the specified changes.
   *
   * @param ident The identifier of the schema to alter.
   * @param changes The changes to apply to the schema.
   * @return The altered {@link HiveSchema}.
   * @throws NoSuchSchemaException If the schema with the provided identifier does not exist.
   */
  @Override
  public HiveSchema alterSchema(NameIdentifier ident, SchemaChange... changes)
      throws NoSuchSchemaException {
    Preconditions.checkArgument(
        !ident.name().isEmpty(),
        String.format("Cannot create schema with invalid name: %s", ident.name()));
    Preconditions.checkArgument(
        isValidNamespace(ident.namespace()),
        String.format("Cannot support invalid namespace in Hive Metastore: %s", ident.namespace()));

    try {
      // load the database parameters
      Database database = clientPool.run(client -> client.getDatabase(ident.name()));
      Map<String, String> metadata = HiveSchema.convertToMetadata(database);
      LOG.debug(
          "Loaded metadata for Hive schema (database) {} found {}",
          ident.name(),
          metadata.keySet());

      for (SchemaChange change : changes) {
        if (change instanceof SchemaChange.SetProperty) {
          metadata.put(
              ((SchemaChange.SetProperty) change).getProperty(),
              ((SchemaChange.SetProperty) change).getValue());
        } else if (change instanceof SchemaChange.RemoveProperty) {
          metadata.remove(((SchemaChange.RemoveProperty) change).getProperty());
        } else {
          throw new IllegalArgumentException(
              "Unsupported schema change type: " + change.getClass().getSimpleName());
        }
      }

      // alter the hive database parameters
      Database alteredDatabase = database.deepCopy();
      alteredDatabase.setParameters(metadata);

      clientPool.run(
          client -> {
            client.alterDatabase(ident.name(), alteredDatabase);
            return null;
          });

      // TODO. We should also update the customized HiveSchema entity fields into our own if
      //  necessary
      HiveSchema.Builder builder = new HiveSchema.Builder();
      builder =
          builder
              .withId(1L /* TODO. Fetch id from underlying storage */)
              .withCatalogId(entity.getId())
              .withNamespace(ident.namespace())
              .withAuditInfo(
                  /* TODO. Fetch audit info from underlying storage */
                  new AuditInfo.Builder()
                      .withCreator(currentUser())
                      .withCreateTime(Instant.now())
                      .withLastModifier(currentUser())
                      .withLastModifiedTime(Instant.now())
                      .build())
              .withConf(hiveConf);
      HiveSchema hiveSchema = HiveSchema.fromInnerDB(alteredDatabase, builder);

      LOG.info("Altered Hive schema (database) {} in Hive Metastore", ident.name());
      // todo(xun): hive does not support renaming database name directly,
      //  perhaps we can use namespace to mapping the database names indirectly

      return hiveSchema;

    } catch (TException | InterruptedException e) {
      throw new RuntimeException(e);
    }
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
    if (ident.name().isEmpty()) {
      LOG.error("Cannot drop schema with invalid name: {}", ident.name());
      return false;
    }
    if (!isValidNamespace(ident.namespace())) {
      LOG.error("Cannot support invalid namespace in Hive Metastore: {}", ident.namespace());
      return false;
    }

    try {
      clientPool.run(
          client -> {
            client.dropDatabase(ident.name(), false, false, cascade);
            return null;
          });

      // TODO. we should also delete the Hive schema (database) from our own underlying storage

      LOG.info("Dropped Hive schema (database) {}", ident.name());
      return true;

    } catch (InvalidOperationException e) {
      throw new NonEmptySchemaException(
          String.format(
              "Hive schema (database) %s is not empty. One or more tables exist.", ident.name()),
          e);

    } catch (NoSuchObjectException e) {
      LOG.warn("Hive schema (database) {} does not exist in Hive Metastore", ident.name());
      return false;

    } catch (TException e) {
      throw new RuntimeException(
          "Failed to drop Hive schema (database) " + ident.name() + " in Hive Metastore", e);

    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
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
    NameIdentifier schemaIdent = NameIdentifier.of(namespace.levels());
    if (!schemaExists(schemaIdent)) {
      throw new NoSuchSchemaException("Schema (database) does not exist " + namespace);
    }

    try {
      // When a table is created using the HMS interface without specifying the `tableType`,
      // although Hive treats it as a `MANAGED_TABLE`, it cannot be queried through the `getTable`
      // interface in HMS with the specified `tableType`. This is because when creating a table
      // without  specifying the `tableType`, the underlying engine of HMS does not store the
      // information of `tableType`. However, once the `getTable` interface specifies a
      // `tableType`, HMS will use it as a filter condition to query its underlying storage and
      // these types of tables will be  filtered out.
      // Therefore, in order to avoid missing these types of tables, we need to query HMS twice. The
      // first time is to retrieve all types of table names (including the missing type tables), and
      // then based on
      // those names we can obtain metadata for each individual table and get the type we needed.
      List<String> allTables = clientPool.run(c -> c.getAllTables(schemaIdent.name()));
      return clientPool.run(
          c ->
              c.getTableObjectsByName(schemaIdent.name(), allTables).stream()
                  .filter(tb -> SUPPORT_TABLE_TYPES.contains(tb.getTableType()))
                  .map(tb -> NameIdentifier.of(namespace, tb.getTableName()))
                  .toArray(NameIdentifier[]::new));
    } catch (UnknownDBException e) {
      throw new NoSuchSchemaException(
          "Schema (database) does not exist " + namespace + " in Hive Metastore");
    } catch (TException e) {
      throw new RuntimeException(
          "Failed to list all tables under the namespace : " + namespace + " in Hive Metastore", e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Loads a table from the Hive Metastore.
   *
   * @param tableIdent The identifier of the table to load.
   * @return The loaded HiveTable instance representing the table.
   * @throws NoSuchTableException If the specified table does not exist in the Hive Metastore.
   */
  @Override
  public Table loadTable(NameIdentifier tableIdent) throws NoSuchTableException {
    Preconditions.checkArgument(!tableIdent.name().isEmpty(), "Cannot load table with empty name");

    NameIdentifier schemaIdent = NameIdentifier.of(tableIdent.namespace().levels());
    Preconditions.checkArgument(
        isValidNamespace(schemaIdent.namespace()),
        String.format(
            "Cannot support invalid namespace in Hive Metastore: %s", schemaIdent.namespace()));

    HiveSchema schema = loadSchema(schemaIdent);
    try {
      org.apache.hadoop.hive.metastore.api.Table hiveTable =
          clientPool.run(c -> c.getTable(schemaIdent.name(), tableIdent.name()));
      HiveTable.Builder builder = new HiveTable.Builder();

      // TODO: We should also fetch the customized HiveTable entity fields from our own
      //  underlying storage, like id, auditInfo, etc.

      builder =
          builder
              .withId(1L /* TODO: Fetch id from underlying storage */)
              .withSchemaId((Long) schema.fields().get(BaseSchema.ID))
              .withName(tableIdent.name())
              .withNameSpace(tableIdent.namespace())
              .withAuditInfo(
                  /* TODO: Fetch audit info from underlying storage */
                  new AuditInfo.Builder()
                      .withCreator(currentUser())
                      .withCreateTime(Instant.now())
                      .build());
      HiveTable table = HiveTable.fromInnerTable(hiveTable, builder);

      LOG.info("Loaded Hive table {} from Hive Metastore ", tableIdent.name());

      return table;
    } catch (TException e) {
      throw new NoSuchTableException(
          String.format("Hive table does not exist: %s in Hive Metastore", tableIdent.name()), e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Creates a new table in the Hive Metastore.
   *
   * @param tableIdent The identifier of the table to create.
   * @param columns The array of columns for the new table.
   * @param comment The comment for the new table.
   * @param properties The properties for the new table.
   * @return The newly created HiveTable instance.
   * @throws NoSuchSchemaException If the schema for the table does not exist.
   * @throws TableAlreadyExistsException If the table with the same name already exists.
   */
  @Override
  public Table createTable(
      NameIdentifier tableIdent, Column[] columns, String comment, Map<String, String> properties)
      throws NoSuchSchemaException, TableAlreadyExistsException {
    Preconditions.checkArgument(
        !tableIdent.name().isEmpty(), "Cannot create table with empty name");

    NameIdentifier schemaIdent = NameIdentifier.of(tableIdent.namespace().levels());
    Preconditions.checkArgument(
        isValidNamespace(schemaIdent.namespace()),
        String.format(
            "Cannot support invalid namespace in Hive Metastore: %s", schemaIdent.namespace()));

    try {
      HiveSchema schema = loadSchema(schemaIdent);

      HiveTable table =
          new HiveTable.Builder()
              .withId(1L /* TODO: Use ID generator*/)
              .withSchemaId((Long) schema.fields().get(BaseSchema.ID))
              .withName(tableIdent.name())
              .withNameSpace(tableIdent.namespace())
              .withColumns(columns)
              .withComment(comment)
              .withProperties(properties)
              .withAuditInfo(
                  new AuditInfo.Builder()
                      .withCreator(currentUser())
                      .withCreateTime(Instant.now())
                      .build())
              .build();
      clientPool.run(
          c -> {
            c.createTable(table.toInnerTable());
            return null;
          });

      // TODO. We should also store the customized HiveTable entity fields into our own
      //  underlying storage, like id, auditInfo, etc.

      LOG.info("Created Hive table {} in Hive Metastore", tableIdent.name());

      return table;

    } catch (AlreadyExistsException e) {
      throw new TableAlreadyExistsException("Table already exists: " + tableIdent.name(), e);
    } catch (TException e) {
      throw new RuntimeException(
          "Failed to create Hive table " + tableIdent.name() + " in Hive Metastore", e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Not supported in this implementation. Throws UnsupportedOperationException.
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
    Preconditions.checkArgument(
        !tableIdent.name().isEmpty(), "Cannot create table with empty name");

    NameIdentifier schemaIdent = NameIdentifier.of(tableIdent.namespace().levels());
    Preconditions.checkArgument(
        isValidNamespace(schemaIdent.namespace()),
        String.format(
            "Cannot support invalid namespace in Hive Metastore: %s", schemaIdent.namespace()));

    try {
      // TODO(@Minghuang): require a table lock to avoid race condition
      HiveTable table = (HiveTable) loadTable(tableIdent);
      org.apache.hadoop.hive.metastore.api.Table alteredHiveTable = table.toInnerTable();

      for (TableChange change : changes) {
        // Table change
        if (change instanceof TableChange.RenameTable) {
          doRenameTable(alteredHiveTable, (TableChange.RenameTable) change);

        } else if (change instanceof TableChange.UpdateComment) {
          doUpdateComment(alteredHiveTable, (TableChange.UpdateComment) change);

        } else if (change instanceof TableChange.SetProperty) {
          doSetProperty(alteredHiveTable, (TableChange.SetProperty) change);

        } else if (change instanceof TableChange.RemoveProperty) {
          doRemoveProperty(alteredHiveTable, (TableChange.RemoveProperty) change);

        } else if (change instanceof TableChange.ColumnChange) {
          // Column change
          StorageDescriptor sd = alteredHiveTable.getSd();
          List<FieldSchema> cols = sd.getCols();

          if (change instanceof TableChange.AddColumn) {
            doAddColumn(cols, (TableChange.AddColumn) change);

          } else if (change instanceof TableChange.DeleteColumn) {
            doDeleteColumn(cols, (TableChange.DeleteColumn) change);

          } else if (change instanceof TableChange.RenameColumn) {
            doRenameColumn(cols, (TableChange.RenameColumn) change);

          } else if (change instanceof TableChange.UpdateColumnComment) {
            doUpdateColumnComment(cols, (TableChange.UpdateColumnComment) change);

          } else if (change instanceof TableChange.UpdateColumnPosition) {
            doUpdateColumnPosition(cols, (TableChange.UpdateColumnPosition) change);

          } else if (change instanceof TableChange.UpdateColumnType) {
            doUpdateColumnType(cols, (TableChange.UpdateColumnType) change);

          } else {
            throw new IllegalArgumentException(
                "Unsupported column change type: " + change.getClass().getSimpleName());
          }
        } else {
          throw new IllegalArgumentException(
              "Unsupported table change type: " + change.getClass().getSimpleName());
        }
      }

      clientPool.run(
          c -> {
            c.alter_table(schemaIdent.name(), tableIdent.name(), alteredHiveTable);
            return null;
          });

      // TODO(@Minghuang). We should also update the customized HiveTable entity fields into our own
      //  if necessary
      HiveTable.Builder builder = new HiveTable.Builder();
      builder =
          builder
              .withId((Long) table.fields().get(BaseTable.ID))
              .withSchemaId((Long) table.fields().get(BaseTable.SCHEMA_ID))
              .withName(alteredHiveTable.getTableName())
              .withNameSpace(tableIdent.namespace())
              .withAuditInfo(
                  /* TODO(@Minghuang): Fetch audit info from underlying storage */
                  new AuditInfo.Builder()
                      .withCreator(currentUser())
                      .withCreateTime(Instant.now())
                      .build());
      HiveTable alteredTable = HiveTable.fromInnerTable(alteredHiveTable, builder);
      LOG.info("Altered Hive table {} in Hive Metastore", tableIdent.name());

      return alteredTable;

    } catch (NoSuchObjectException e) {
      throw new NoSuchTableException(
          String.format("Hive table does not exist: %s in Hive Metastore", tableIdent.name()), e);
    } catch (TException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private int columnPosition(List<FieldSchema> columns, TableChange.ColumnPosition position) {
    if (position == null) {
      // add to the end by default
      return columns.size();
    } else if (position instanceof TableChange.After) {
      String afterColumn = ((TableChange.After) position).getColumn();
      return indexOfColumn(columns, afterColumn) + 1;
    }
    return 0;
  }

  /**
   * Returns the index of a column in the given list of FieldSchema objects, based on the field
   * name.
   *
   * @param columns The list of Hive columns.
   * @param fieldName The name of the field to be searched.
   * @return The index of the column if found, otherwise -1.
   */
  private int indexOfColumn(List<FieldSchema> columns, String fieldName) {
    return columns.stream()
        .map(FieldSchema::getName)
        .collect(Collectors.toList())
        .indexOf(fieldName);
  }

  private void doRenameTable(
      org.apache.hadoop.hive.metastore.api.Table hiveTable, TableChange.RenameTable change) {
    hiveTable.setTableName(change.getNewName());
  }

  private void doUpdateComment(
      org.apache.hadoop.hive.metastore.api.Table hiveTable, TableChange.UpdateComment change) {
    Map<String, String> parameters = hiveTable.getParameters();
    parameters.put(HMS_TABLE_COMMENT, change.getNewComment());
  }

  private void doSetProperty(
      org.apache.hadoop.hive.metastore.api.Table hiveTable, TableChange.SetProperty change) {
    Map<String, String> parameters = hiveTable.getParameters();
    parameters.put(change.getProperty(), change.getValue());
  }

  private void doRemoveProperty(
      org.apache.hadoop.hive.metastore.api.Table hiveTable, TableChange.RemoveProperty change) {
    Map<String, String> parameters = hiveTable.getParameters();
    parameters.remove(change.getProperty());
  }

  void doAddColumn(List<FieldSchema> cols, TableChange.AddColumn change) {
    cols.add(
        columnPosition(cols, change.getPosition()),
        new FieldSchema(
            change.fieldNames()[0],
            change.getDataType().accept(ToHiveType.INSTANCE).getQualifiedName(),
            change.getComment()));
  }

  private void doDeleteColumn(List<FieldSchema> cols, TableChange.DeleteColumn change) {
    String columnName = change.fieldNames()[0];
    if (!cols.removeIf(c -> c.getName().equals(columnName)) && !change.getIfExists()) {
      throw new IllegalArgumentException("DeleteColumn does not exist: " + columnName);
    }
  }

  private void doRenameColumn(List<FieldSchema> cols, TableChange.RenameColumn change) {
    String columnName = change.fieldNames()[0];
    if (indexOfColumn(cols, columnName) == -1) {
      throw new IllegalArgumentException("RenameColumn does not exist: " + columnName);
    }

    String newName = change.getNewName();
    if (indexOfColumn(cols, newName) != -1) {
      throw new IllegalArgumentException("Column already exists: " + newName);
    }
    cols.get(indexOfColumn(cols, columnName)).setName(newName);
  }

  private void doUpdateColumnComment(
      List<FieldSchema> cols, TableChange.UpdateColumnComment change) {
    cols.get(indexOfColumn(cols, change.fieldNames()[0])).setComment(change.getNewComment());
  }

  private void doUpdateColumnPosition(
      List<FieldSchema> cols, TableChange.UpdateColumnPosition change) {
    String columnName = change.fieldNames()[0];
    int sourceIndex = indexOfColumn(cols, columnName);
    if (sourceIndex == -1) {
      throw new IllegalArgumentException("UpdateColumnPosition does not exist: " + columnName);
    }

    // update column position: remove then add to given position
    FieldSchema hiveColumn = cols.remove(sourceIndex);
    cols.add(columnPosition(cols, change.getPosition()), hiveColumn);
  }

  private void doUpdateColumnType(List<FieldSchema> cols, TableChange.UpdateColumnType change) {
    String columnName = change.fieldNames()[0];
    int indexOfColumn = indexOfColumn(cols, columnName);
    if (indexOfColumn == -1) {
      throw new IllegalArgumentException("UpdateColumnType does not exist: " + columnName);
    }
    cols.get(indexOfColumn)
        .setType(change.getNewDataType().accept(ToHiveType.INSTANCE).getQualifiedName());
  }

  /**
   * Drops a table from the Hive Metastore.
   *
   * @param tableIdent The identifier of the table to drop.
   * @return true if the table is successfully dropped; false if the table does not exist.
   */
  @Override
  public boolean dropTable(NameIdentifier tableIdent) {
    return dropHiveTable(tableIdent, false, false);
  }

  /**
   * Purges a table from the Hive Metastore.
   *
   * @param tableIdent The identifier of the table to purge.
   * @return true if the table is successfully purged; false if the table does not exist.
   * @throws UnsupportedOperationException If the table type is EXTERNAL_TABLE, it cannot be purged.
   */
  @Override
  public boolean purgeTable(NameIdentifier tableIdent) throws UnsupportedOperationException {
    HiveTable table = (HiveTable) loadTable(tableIdent);
    // TODO(minghuang): HiveTable support specify `tableType`, then reject purge Hive table with
    //  `tableType` EXTERNAL_TABLE
    return dropHiveTable(tableIdent, true, true);
  }

  /**
   * Checks if the given namespace is a valid namespace for the Hive schema.
   *
   * @param tableIdent The namespace to validate.
   * @return true if the namespace is valid; otherwise, false.
   */
  private boolean dropHiveTable(NameIdentifier tableIdent, boolean deleteData, boolean ifPurge) {
    Preconditions.checkArgument(!tableIdent.name().isEmpty(), "Cannot drop table with empty name");

    NameIdentifier schemaIdent = NameIdentifier.of(tableIdent.namespace().levels());
    Preconditions.checkArgument(
        isValidNamespace(schemaIdent.namespace()),
        String.format(
            "Cannot support invalid namespace in Hive Metastore: %s", schemaIdent.namespace()));

    try {
      clientPool.run(
          c -> {
            c.dropTable(schemaIdent.name(), tableIdent.name(), deleteData, false, ifPurge);
            return null;
          });

      // TODO. we should also delete the Hive table from our own underlying storage

      LOG.info("Dropped Hive table {}", tableIdent.name());
      return true;

    } catch (NoSuchObjectException e) {
      LOG.warn("Hive table {} does not exist in Hive Metastore", tableIdent.name());
      return false;
    } catch (TException e) {
      throw new RuntimeException(
          "Failed to drop Hive table " + tableIdent.name() + " in Hive Metastore", e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Checks if the given namespace is a valid namespace for the Hive schema.
   *
   * @param namespace The namespace to validate.
   * @return true if the namespace is valid; otherwise, false.
   */
  public boolean isValidNamespace(Namespace namespace) {
    return namespace.levels().length == 2 && namespace.level(1).equals(entity.name());
  }

  // TODO. We should figure out a better way to get the current user from servlet container.
  private static String currentUser() {
    String username = null;
    try {
      username = UserGroupInformation.getCurrentUser().getShortUserName();
    } catch (IOException e) {
      LOG.warn("Failed to get Hadoop user", e);
    }

    if (username != null) {
      return username;
    } else {
      LOG.warn("Hadoop user is null, defaulting to user.name");
      return System.getProperty("user.name");
    }
  }
}
