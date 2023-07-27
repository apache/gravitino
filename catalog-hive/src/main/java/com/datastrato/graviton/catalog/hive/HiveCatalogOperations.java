/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.catalog.hive;

import static com.datastrato.graviton.catalog.hive.HiveTable.SUPPORT_TABLE_TYPES;
import static org.apache.hadoop.hive.metastore.TableType.EXTERNAL_TABLE;

import com.datastrato.graviton.NameIdentifier;
import com.datastrato.graviton.Namespace;
import com.datastrato.graviton.catalog.CatalogOperations;
import com.datastrato.graviton.exceptions.NoSuchNamespaceException;
import com.datastrato.graviton.exceptions.NoSuchSchemaException;
import com.datastrato.graviton.exceptions.NoSuchTableException;
import com.datastrato.graviton.exceptions.NonEmptySchemaException;
import com.datastrato.graviton.exceptions.SchemaAlreadyExistsException;
import com.datastrato.graviton.exceptions.TableAlreadyExistsException;
import com.datastrato.graviton.meta.AuditInfo;
import com.datastrato.graviton.meta.CatalogEntity;
import com.datastrato.graviton.meta.rel.BaseSchema;
import com.datastrato.graviton.rel.Column;
import com.datastrato.graviton.rel.SchemaChange;
import com.datastrato.graviton.rel.SupportsSchemas;
import com.datastrato.graviton.rel.Table;
import com.datastrato.graviton.rel.TableCatalog;
import com.datastrato.graviton.rel.TableChange;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveCatalogOperations implements CatalogOperations, SupportsSchemas, TableCatalog {

  public static final Logger LOG = LoggerFactory.getLogger(HiveCatalogOperations.class);

  @VisibleForTesting HiveClientPool clientPool;

  private HiveConf hiveConf;

  private final CatalogEntity entity;

  public HiveCatalogOperations(CatalogEntity entity) {
    this.entity = entity;
  }

  @Override
  public void initialize(Map<String, String> conf) throws RuntimeException {
    Configuration hadoopConf = new Configuration();
    conf.forEach(hadoopConf::set);
    hiveConf = new HiveConf(hadoopConf, HiveCatalogOperations.class);

    // todo(xun): add hive client pool size in config
    this.clientPool = new HiveClientPool(1, hiveConf);
  }

  @Override
  public void close() {
    if (clientPool != null) {
      clientPool.close();
      clientPool = null;
    }
  }

  @Override
  public NameIdentifier[] listSchemas(Namespace namespace) throws NoSuchNamespaceException {
    if (!isValidNamespace(namespace)) {
      throw new NoSuchNamespaceException("Namespace is invalid " + namespace);
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

  @Override
  public HiveSchema createSchema(NameIdentifier ident, String comment, Map<String, String> metadata)
      throws SchemaAlreadyExistsException {
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

  @Override
  public NameIdentifier[] listTables(Namespace namespace) throws NoSuchSchemaException {
    NameIdentifier schemaIdent = NameIdentifier.of(namespace.levels());
    if (!schemaExists(schemaIdent)) {
      throw new NoSuchSchemaException("Schema (database) does not exist " + namespace);
    }

    try {
      List<NameIdentifier> tables = Lists.newArrayList();

      for (TableType tableType : SUPPORT_TABLE_TYPES) {
        tables.addAll(
            clientPool.run(
                c ->
                    c.getTables(schemaIdent.name(), "*", tableType).stream()
                        .map(tb -> NameIdentifier.of(namespace, tb))
                        .collect(Collectors.toList())));
      }

      return tables.toArray(new NameIdentifier[0]);
    } catch (TException e) {
      throw new RuntimeException(
          "Failed to list all tables under under namespace : " + namespace + " in Hive Metastore",
          e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

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

  @Override
  public Table alterTable(NameIdentifier ident, TableChange... changes)
      throws NoSuchTableException, IllegalArgumentException {
    throw new UnsupportedOperationException("Not support alter Hive table yet");
  }

  @Override
  public boolean dropTable(NameIdentifier tableIdent) {
    return dropHiveTable(tableIdent, false, false);
  }

  @Override
  public boolean purgeTable(NameIdentifier tableIdent) throws UnsupportedOperationException {
    HiveTable table = (HiveTable) loadTable(tableIdent);
    if (EXTERNAL_TABLE == table.getTableType()) {
      throw new UnsupportedOperationException("Cannot purge Hive table with type EXTERNAL_TABLE");
    }
    return dropHiveTable(tableIdent, true, true);
  }

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
   * Valid namespace of Hive schema
   *
   * @param namespace of Hive scheme
   * @return true if catalog name equals current
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
