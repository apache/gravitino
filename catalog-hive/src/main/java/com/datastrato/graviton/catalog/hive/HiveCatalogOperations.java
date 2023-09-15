/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.catalog.hive;

import static com.datastrato.graviton.Entity.EntityType.SCHEMA;
import static com.datastrato.graviton.Entity.EntityType.TABLE;
import static com.datastrato.graviton.catalog.hive.HiveTable.HMS_TABLE_COMMENT;
import static com.datastrato.graviton.catalog.hive.HiveTable.SUPPORT_TABLE_TYPES;

import com.datastrato.graviton.EntityAlreadyExistsException;
import com.datastrato.graviton.EntityStore;
import com.datastrato.graviton.GravitonEnv;
import com.datastrato.graviton.NameIdentifier;
import com.datastrato.graviton.Namespace;
import com.datastrato.graviton.StringIdentifier;
import com.datastrato.graviton.catalog.CatalogOperations;
import com.datastrato.graviton.catalog.hive.converter.ToHiveType;
import com.datastrato.graviton.exceptions.NoSuchCatalogException;
import com.datastrato.graviton.exceptions.NoSuchEntityException;
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
import com.datastrato.graviton.rel.transforms.Transform;
import com.datastrato.graviton.rel.transforms.Transforms;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.ArrayUtils;
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
      EntityStore store = GravitonEnv.getInstance().entityStore();
      long uid = GravitonEnv.getInstance().idGenerator().nextId();
      StringIdentifier stringId = StringIdentifier.fromId(uid);

      HiveSchema hiveSchema =
          store.executeInTransaction(
              () -> {
                HiveSchema createdSchema =
                    new HiveSchema.Builder()
                        .withId(uid)
                        .withName(ident.name())
                        .withNamespace(ident.namespace())
                        .withComment(comment)
                        .withProperties(StringIdentifier.addToProperties(stringId, metadata))
                        .withAuditInfo(
                            new AuditInfo.Builder()
                                .withCreator(currentUser())
                                .withCreateTime(Instant.now())
                                .build())
                        .withConf(hiveConf)
                        .build();
                store.put(createdSchema, false);
                clientPool.run(
                    client -> {
                      client.createDatabase(createdSchema.toInnerDB());
                      return null;
                    });
                return createdSchema;
              });

      LOG.info("Created Hive schema (database) {} in Hive Metastore", ident.name());

      return hiveSchema;

    } catch (AlreadyExistsException e) {
      throw new SchemaAlreadyExistsException(
          String.format(
              "Hive schema (database) '%s' already exists in Hive Metastore", ident.name()),
          e);

    } catch (EntityAlreadyExistsException e) {
      throw new SchemaAlreadyExistsException(
          String.format(
              "Hive schema (database) '%s' already exists in Graviton store", ident.name()),
          e);

    } catch (TException e) {
      throw new RuntimeException(
          "Failed to create Hive schema (database) " + ident.name() + " in Hive Metastore", e);

    } catch (IOException e) {
      throw new RuntimeException(
          "Failed to create Hive schema (database) " + ident.name() + " in Graviton store", e);

    } catch (Exception e) {
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

      EntityStore store = GravitonEnv.getInstance().entityStore();
      BaseSchema baseSchema = store.get(ident, SCHEMA, BaseSchema.class);

      builder = builder.withId(baseSchema.id()).withNamespace(ident.namespace()).withConf(hiveConf);
      HiveSchema hiveSchema = HiveSchema.fromInnerDB(database, builder);

      // Merge audit info from Graviton store
      hiveSchema.auditInfo().merge(baseSchema.auditInfo(), true /*overwrite*/);

      LOG.info("Loaded Hive schema (database) {} from Hive Metastore ", ident.name());

      return hiveSchema;

    } catch (NoSuchObjectException | UnknownDBException e) {
      deleteSchemaFromStore(ident);
      throw new NoSuchSchemaException(
          String.format(
              "Hive schema (database) does not exist: %s in Hive Metastore", ident.name()),
          e);

    } catch (NoSuchEntityException e) {
      throw new NoSuchSchemaException(
          String.format(
              "Hive schema (database) does not exist: %s in Graviton store", ident.name()),
          e);

    } catch (TException e) {
      throw new RuntimeException(
          "Failed to load Hive schema (database) " + ident.name() + " from Hive Metastore", e);

    } catch (InterruptedException e) {
      throw new RuntimeException(e);

    } catch (IOException ioe) {
      LOG.error("Failed to load hive schema {}", ident, ioe);
      throw new RuntimeException(ioe);
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
      // Note: Hive will ignore case issues, if the name(ident.name()) of the schema is
      // upper-case, then the name hive schema will be lower-case, Which will cause a problem,
      // we need to set the name of the hiveSchema to the same as the name of the ident.
      // Note: always use the name of the ident to update the hive schema name.
      alteredDatabase.setName(ident.name());
      alteredDatabase.setParameters(metadata);

      // Update store in a transaction
      EntityStore store = GravitonEnv.getInstance().entityStore();
      HiveSchema alteredHiveSchema =
          store.executeInTransaction(
              () -> {
                BaseSchema oldSchema = store.get(ident, SCHEMA, BaseSchema.class);
                HiveSchema.Builder builder = new HiveSchema.Builder();
                builder =
                    builder
                        .withId(oldSchema.id())
                        .withNamespace(ident.namespace())
                        .withConf(hiveConf);
                HiveSchema hiveSchema = HiveSchema.fromInnerDB(alteredDatabase, builder);

                AuditInfo newAudit =
                    new AuditInfo.Builder()
                        .withCreator(oldSchema.auditInfo().creator())
                        .withCreateTime(oldSchema.auditInfo().createTime())
                        .withLastModifier(currentUser())
                        .withLastModifiedTime(Instant.now())
                        .build();
                hiveSchema.auditInfo().merge(newAudit, true /*overwrite*/);

                // To be on the safe side, here uses delete before put (although  hive schema does
                // not support rename yet)
                store.delete(ident, SCHEMA);
                store.put(hiveSchema, false);
                clientPool.run(
                    client -> {
                      client.alterDatabase(ident.name(), alteredDatabase);
                      return null;
                    });
                return hiveSchema;
              });

      LOG.info("Altered Hive schema (database) {} in Hive Metastore", ident.name());
      // todo(xun): hive does not support renaming database name directly,
      //  perhaps we can use namespace to mapping the database names indirectly

      return alteredHiveSchema;

    } catch (NoSuchObjectException e) {
      throw new NoSuchSchemaException(
          String.format("Hive schema (database) %s does not exist in Hive Metastore", ident.name()),
          e);

    } catch (EntityAlreadyExistsException e) {
      throw new NoSuchSchemaException(
          "The new Hive schema (database) name already exist in Graviton store", e);

    } catch (TException | InterruptedException e) {
      throw new RuntimeException(
          "Failed to alter Hive schema (database) " + ident.name() + " in Hive metastore", e);

    } catch (IOException e) {
      throw new RuntimeException(
          "Failed to alter Hive schema (database) " + ident.name() + " in Graviton store", e);

    } catch (Exception e) {
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

    EntityStore store = GravitonEnv.getInstance().entityStore();
    Namespace schemaNamespace =
        Namespace.of(ArrayUtils.add(ident.namespace().levels(), ident.name()));
    List<BaseTable> tables = Lists.newArrayList();
    if (!cascade) {
      if (listTables(schemaNamespace).length > 0) {
        throw new NonEmptySchemaException(
            String.format(
                "Hive schema (database) %s is not empty. One or more tables exist in Hive metastore.",
                ident.name()));
      }

      try {
        tables.addAll(store.list(schemaNamespace, BaseTable.class, TABLE));
      } catch (IOException e) {
        throw new RuntimeException("Failed to list table from Graviton store", e);
      }
      if (!tables.isEmpty()) {
        throw new NonEmptySchemaException(
            String.format(
                "Hive schema (database) %s is not empty. One or more tables exist in Graviton store.",
                ident.name()));
      }
    }

    try {
      return store.executeInTransaction(
          () -> {
            for (BaseTable t : tables) {
              store.delete(NameIdentifier.of(schemaNamespace, t.name()), TABLE);
            }
            boolean dropped = store.delete(ident, SCHEMA, true);
            clientPool.run(
                client -> {
                  client.dropDatabase(ident.name(), false, false, cascade);
                  return null;
                });
            LOG.info("Dropped Hive schema (database) {}", ident.name());
            return dropped;
          });

    } catch (InvalidOperationException e) {
      throw new NonEmptySchemaException(
          String.format(
              "Hive schema (database) %s is not empty. One or more tables exist.", ident.name()),
          e);

    } catch (NoSuchObjectException e) {
      deleteSchemaFromStore(ident);
      LOG.warn("Hive schema (database) {} does not exist in Hive Metastore", ident.name());
      return false;

    } catch (TException e) {
      throw new RuntimeException(
          "Failed to drop Hive schema (database) " + ident.name() + " in Hive Metastore", e);

    } catch (IOException e) {
      throw new RuntimeException(
          "Failed to drop Hive schema (database) " + ident.name() + " in Graviton store", e);

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void deleteSchemaFromStore(NameIdentifier ident) {
    EntityStore store = GravitonEnv.getInstance().entityStore();
    try {
      store.delete(ident, SCHEMA);
    } catch (IOException ex) {
      LOG.error("Failed to delete hive schema {} from Graviton store", ident, ex);
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

    try {
      org.apache.hadoop.hive.metastore.api.Table hiveTable =
          clientPool.run(c -> c.getTable(schemaIdent.name(), tableIdent.name()));
      HiveTable.Builder builder = new HiveTable.Builder();

      EntityStore store = GravitonEnv.getInstance().entityStore();
      BaseTable baseTable = store.get(tableIdent, TABLE, BaseTable.class);

      builder =
          builder
              .withId(baseTable.id())
              .withName(tableIdent.name())
              .withNameSpace(tableIdent.namespace());
      HiveTable table = HiveTable.fromInnerTable(hiveTable, builder);

      // Merge the audit info from Graviton store.
      table.auditInfo().merge(baseTable.auditInfo(), true /* overwrite */);

      LOG.info("Loaded Hive table {} from Hive Metastore ", tableIdent.name());

      return table;
    } catch (NoSuchObjectException e) {
      throw new NoSuchTableException(
          String.format("Hive table does not exist: %s in Hive Metastore", tableIdent.name()), e);

    } catch (InterruptedException | TException e) {
      throw new RuntimeException(
          "Failed to load Hive table " + tableIdent.name() + " from Hive metastore", e);

    } catch (IOException e) {
      throw new RuntimeException(
          "Failed to load Hive table " + tableIdent.name() + " from Graviton store", e);
    }
  }

  /**
   * Creates a new table in the Hive Metastore.
   *
   * @param tableIdent The identifier of the table to create.
   * @param columns The array of columns for the new table.
   * @param comment The comment for the new table.
   * @param properties The properties for the new table.
   * @param partitions The partitioning for the new table.
   * @return The newly created HiveTable instance.
   * @throws NoSuchSchemaException If the schema for the table does not exist.
   * @throws TableAlreadyExistsException If the table with the same name already exists.
   */
  @Override
  public Table createTable(
      NameIdentifier tableIdent,
      Column[] columns,
      String comment,
      Map<String, String> properties,
      Transform[] partitions)
      throws NoSuchSchemaException, TableAlreadyExistsException {
    Preconditions.checkArgument(
        !tableIdent.name().isEmpty(), "Cannot create table with empty name");

    NameIdentifier schemaIdent = NameIdentifier.of(tableIdent.namespace().levels());
    Preconditions.checkArgument(
        isValidNamespace(schemaIdent.namespace()),
        String.format(
            "Cannot support invalid namespace in Hive Metastore: %s", schemaIdent.namespace()));

    Preconditions.checkArgument(
        Arrays.stream(partitions).allMatch(p -> p instanceof Transforms.NamedReference),
        "Hive partition only supports identity transform");

    try {
      if (!schemaExists(schemaIdent)) {
        LOG.warn("Hive schema (database) does not exist: {}", schemaIdent);
        throw new NoSuchSchemaException("Hive Schema (database) does not exist " + schemaIdent);
      }

      EntityStore store = GravitonEnv.getInstance().entityStore();
      long uid = GravitonEnv.getInstance().idGenerator().nextId();
      StringIdentifier stringId = StringIdentifier.fromId(uid);

      HiveTable hiveTable =
          store.executeInTransaction(
              () -> {
                HiveTable createdTable =
                    new HiveTable.Builder()
                        .withId(uid)
                        .withName(tableIdent.name())
                        .withNameSpace(tableIdent.namespace())
                        .withColumns(columns)
                        .withComment(comment)
                        .withProperties(StringIdentifier.addToProperties(stringId, properties))
                        .withAuditInfo(
                            new AuditInfo.Builder()
                                .withCreator(currentUser())
                                .withCreateTime(Instant.now())
                                .build())
                        .withPartitions(partitions)
                        .build();
                store.put(createdTable, false);
                clientPool.run(
                    c -> {
                      c.createTable(createdTable.toInnerTable());
                      return null;
                    });
                return createdTable;
              });

      LOG.info("Created Hive table {} in Hive Metastore", tableIdent.name());

      return hiveTable;

    } catch (AlreadyExistsException | EntityAlreadyExistsException e) {
      throw new TableAlreadyExistsException("Table already exists: " + tableIdent.name(), e);
    } catch (TException | InterruptedException e) {
      throw new RuntimeException(
          "Failed to create Hive table " + tableIdent.name() + " in Hive Metastore", e);
    } catch (IOException e) {
      throw new RuntimeException(
          "Failed to create Hive table " + tableIdent.name() + " in Graviton store", e);
    } catch (Exception e) {
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

      EntityStore store = GravitonEnv.getInstance().entityStore();
      HiveTable updatedTable =
          store.executeInTransaction(
              () -> {
                HiveTable.Builder builder = new HiveTable.Builder();
                builder =
                    builder
                        .withId(table.id())
                        .withName(alteredHiveTable.getTableName())
                        .withNameSpace(tableIdent.namespace());

                HiveTable alteredTable = HiveTable.fromInnerTable(alteredHiveTable, builder);

                AuditInfo newAudit =
                    new AuditInfo.Builder()
                        .withCreator(table.auditInfo().creator())
                        .withCreateTime(table.auditInfo().createTime())
                        .withLastModifier(currentUser())
                        .withLastModifiedTime(Instant.now())
                        .build();
                alteredTable.auditInfo().merge(newAudit, true /* overwrite */);

                store.delete(tableIdent, TABLE);
                store.put(alteredTable, false);
                clientPool.run(
                    c -> {
                      c.alter_table(schemaIdent.name(), tableIdent.name(), alteredHiveTable);
                      return null;
                    });
                return alteredTable;
              });

      LOG.info("Altered Hive table {} in Hive Metastore", tableIdent.name());

      return updatedTable;

    } catch (NoSuchObjectException e) {
      throw new NoSuchTableException(
          String.format("Hive table does not exist: %s in Hive Metastore", tableIdent.name()), e);
    } catch (TException | InterruptedException e) {
      throw new RuntimeException(
          "Failed to alter Hive table " + tableIdent.name() + " in Hive metastore", e);
    } catch (IOException e) {
      throw new RuntimeException(
          "Failed to alter Hive table " + tableIdent.name() + " in Graviton store", e);
    } catch (Exception e) {
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
      EntityStore store = GravitonEnv.getInstance().entityStore();
      return store.executeInTransaction(
          () -> {
            boolean dropped = store.delete(tableIdent, TABLE);
            clientPool.run(
                c -> {
                  c.dropTable(schemaIdent.name(), tableIdent.name(), deleteData, false, ifPurge);
                  return null;
                });
            LOG.info("Dropped Hive table {}", tableIdent.name());
            return dropped;
          });
    } catch (NoSuchObjectException e) {
      LOG.warn("Hive table {} does not exist in Hive Metastore", tableIdent.name());
      return false;
    } catch (TException | InterruptedException e) {
      throw new RuntimeException(
          "Failed to drop Hive table " + tableIdent.name() + " in Hive Metastore", e);
    } catch (IOException e) {
      throw new RuntimeException(
          "Failed to drop Hive table " + tableIdent.name() + " in Graviton store", e);
    } catch (Exception e) {
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
