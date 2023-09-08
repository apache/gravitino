/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.catalog.lakehouse.iceberg;

import com.datastrato.graviton.*;
import com.datastrato.graviton.catalog.CatalogOperations;
import com.datastrato.graviton.catalog.lakehouse.iceberg.ops.IcebergTableOps;
import com.datastrato.graviton.exceptions.*;
import com.datastrato.graviton.meta.AuditInfo;
import com.datastrato.graviton.meta.CatalogEntity;
import com.datastrato.graviton.meta.rel.BaseSchema;
import com.datastrato.graviton.meta.rel.BaseTable;
import com.datastrato.graviton.rel.*;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.rest.requests.UpdateNamespacePropertiesRequest;
import org.apache.iceberg.rest.responses.GetNamespaceResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.*;
import java.util.stream.Stream;

import static com.datastrato.graviton.Entity.EntityType.SCHEMA;
import static com.datastrato.graviton.Entity.EntityType.TABLE;

/** Operations for interacting with the Icebergcatalog in Graviton. */
public class IcebergCatalogOperations implements CatalogOperations, SupportsSchemas, TableCatalog {

  public static final Logger LOG = LoggerFactory.getLogger(IcebergCatalogOperations.class);

  private IcebergTableOps icebergTableOps;

  private final CatalogEntity entity;

  /**
   * Constructs a new instance of IcebergCatalogOperations.
   *
   * @param entity The catalog entity associated with this operations instance.
   */
  public IcebergCatalogOperations(CatalogEntity entity) {
    this.entity = entity;
  }

  /**
   * Initializes the Iceberg catalog operations with the provided configuration.
   *
   * @param conf The configuration map for the Iceberg catalog operations.
   * @throws RuntimeException if initialization fails.
   */
  @Override
  public void initialize(Map<String, String> conf) throws RuntimeException {
    //Todo initialize iceberg ops
    this.icebergTableOps = new IcebergTableOps();
  }

  /** Closes the Iceberg catalog and releases the associated client pool. */
  @Override
  public void close() {

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
      return icebergTableOps.listNamespace(
                      org.apache.iceberg.catalog.Namespace.of(namespace.levels())).namespaces()
              .stream()
              .map(icebergNamespace -> NameIdentifier.of(namespace, icebergNamespace.toString()))
              .toArray(NameIdentifier[]::new);
    } catch (NoSuchNamespaceException e) {
      throw new RuntimeException(
          "Failed to list all schemas (database) under namespace : "
              + namespace
              + " in IcebergMetastore",
          e);

    }
  }

  /**
   * Creates a new schema with the provided identifier, comment, and metadata.
   *
   * @param ident The identifier of the schema to create.
   * @param comment The comment for the schema.
   * @param metadata The metadata properties for the schema.
   * @return The created {@link IcebergSchema}.
   * @throws NoSuchCatalogException If the provided namespace is invalid or does not exist.
   * @throws SchemaAlreadyExistsException If a schema with the same name already exists.
   */
  @Override
  public IcebergSchema createSchema(NameIdentifier ident, String comment, Map<String, String> metadata)
      throws NoSuchCatalogException, SchemaAlreadyExistsException {
    Preconditions.checkArgument(
        !ident.name().isEmpty(),
        String.format("Cannot create schema with invalid name: %s", ident.name()));
    Preconditions.checkArgument(
        isValidNamespace(ident.namespace()),
        String.format("Cannot support invalid namespace in IcebergMetastore: %s", ident.namespace()));

    try {
      EntityStore store = GravitonEnv.getInstance().entityStore();
      IcebergSchema icebergSchema =
          store.executeInTransaction(
              () -> {
                IcebergSchema createdSchema = new IcebergSchema.Builder()
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
                        .internalBuild();
                store.put(createdSchema, false);
                icebergTableOps.createNamespace(createdSchema.toCreateRequest());
                return createdSchema;
              });

      LOG.info("Created Iceberg schema (database) {} in IcebergMetastore", ident.name());
      return icebergSchema;
    } catch (org.apache.iceberg.exceptions.AlreadyExistsException e) {
      throw new SchemaAlreadyExistsException(
          String.format(
              "Iceberg schema (database) '%s' already exists in IcebergMetastore", ident.name()),
          e);

    } catch (NoSuchNamespaceException e){
      throw new NoSuchSchemaException(
              String.format(
                      "Iceberg schema (database) does not exist: %s in Graviton store, This scenario occurs after the creation is completed and reloaded", ident.name()),
              e);
    } catch (EntityAlreadyExistsException e) {
      throw new SchemaAlreadyExistsException(
          String.format(
              "Iceberg schema (database) '%s' already exists in Graviton store", ident.name()),
          e);

    } catch (IOException e) {
      throw new RuntimeException(
          "Failed to create Iceberg schema (database) " + ident.name() + " in Graviton store", e);

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Loads the schema with the provided identifier.
   *
   * @param ident The identifier of the schema to load.
   * @return The loaded {@link IcebergSchema}.
   * @throws NoSuchSchemaException If the schema with the provided identifier does not exist.
   */
  @Override
  public IcebergSchema loadSchema(NameIdentifier ident) throws NoSuchSchemaException {
    Preconditions.checkArgument(
        !ident.name().isEmpty(),
        String.format("Cannot load schema with invalid name: %s", ident.name()));
    Preconditions.checkArgument(
        isValidNamespace(ident.namespace()),
        String.format("Cannot support invalid namespace in IcebergMetastore: %s", ident.namespace()));

    try {
      GetNamespaceResponse response = icebergTableOps.loadNamespace(org.apache.iceberg.catalog.Namespace.of(ident.toString()));
      EntityStore store = GravitonEnv.getInstance().entityStore();
      BaseSchema baseSchema = store.get(ident, SCHEMA, BaseSchema.class);
      IcebergSchema icebergSchema = new IcebergSchema.Builder()
              .withId(baseSchema.getId())
              .withCatalogId(entity.getId())
              .withName(ident.name())
              .withNamespace(Namespace.of(response.namespace().levels()))
              .withComment(Optional.of(response).map(GetNamespaceResponse::properties).map(map -> map.get("comment")).orElse(baseSchema.comment()))
              .withProperties(response.properties())
              .withAuditInfo(baseSchema.auditInfo())
              .internalBuild();
      LOG.info("Loaded Iceberg schema (database) {} from IcebergMetastore ", ident.name());
      return icebergSchema;
    } catch (NoSuchNamespaceException e) {
      throw new NoSuchSchemaException(
          String.format(
              "Iceberg schema (database) does not exist: %s in Graviton store", ident.name()),
          e);
    } catch (IOException ioe) {
      LOG.error("Failed to load Iceberg schema {}", ident, ioe);
      throw new RuntimeException(ioe);
    }
  }

  /**
   * Alters the schema with the provided identifier according to the specified changes.
   *
   * @param ident The identifier of the schema to alter.
   * @param changes The changes to apply to the schema.
   * @return The altered {@link IcebergSchema}.
   * @throws NoSuchSchemaException If the schema with the provided identifier does not exist.
   */
  @Override
  public IcebergSchema alterSchema(NameIdentifier ident, SchemaChange... changes)
      throws NoSuchSchemaException {
    Preconditions.checkArgument(
        !ident.name().isEmpty(),
        String.format("Cannot create schema with invalid name: %s", ident.name()));
    Preconditions.checkArgument(
        isValidNamespace(ident.namespace()),
        String.format("Cannot support invalid namespace in IcebergMetastore: %s", ident.namespace()));

    try {
      GetNamespaceResponse response = icebergTableOps.loadNamespace(org.apache.iceberg.catalog.Namespace.of(ident.toString()));
      Map<String, String> metadata = response.properties();
      LOG.debug(
          "Loaded metadata for Iceberg schema (database) {} found {}",
          ident.name(),
          metadata.keySet());
      List<String> removals = new ArrayList<>();
      Map<String, String> updates = new HashMap<>();

      for (SchemaChange change : changes) {
        if (change instanceof SchemaChange.SetProperty) {
          updates.put(
              ((SchemaChange.SetProperty) change).getProperty(),
              ((SchemaChange.SetProperty) change).getValue());
        } else if (change instanceof SchemaChange.RemoveProperty) {
          removals.add(((SchemaChange.RemoveProperty) change).getProperty());
        } else {
          throw new IllegalArgumentException(
              "Unsupported schema change type: " + change.getClass().getSimpleName());
        }
      }

      // update store transactionally
      EntityStore store = GravitonEnv.getInstance().entityStore();
      IcebergSchema alteredIcebergSchema =
          store.executeInTransaction(
              () -> {
                BaseSchema oldSchema = store.get(ident, SCHEMA, BaseSchema.class);

                //Determine whether to change comment.
                String common;
                if (removals.contains("comment")) {
                  common = null;
                } else if (updates.containsKey("comment")) {
                  common = updates.get("comment");
                } else {
                  //Synchronize changes to underlying iceberg sources
                  common = Optional.of(response.properties())
                          .map(map -> map.get("comment")).orElse(null);
                }

                AuditInfo newAudit =
                        new AuditInfo.Builder()
                                .withCreator(oldSchema.auditInfo().creator())
                                .withCreateTime(oldSchema.auditInfo().createTime())
                                .withLastModifier(currentUser())
                                .withLastModifiedTime(Instant.now())
                                .build();
                IcebergSchema icebergSchema = new IcebergSchema.Builder()
                        .withId(oldSchema.getId())
                        .withCatalogId(oldSchema.getCatalogId())
                        .withName(ident.name())
                        .withNamespace(Namespace.of(response.namespace().levels()))
                        .withComment(common)
                        .withProperties(response.properties())
                        .withAuditInfo(newAudit)
                        .internalBuild();

                store.delete(ident, SCHEMA);
                store.put(icebergSchema, false);
                UpdateNamespacePropertiesRequest updateNamespacePropertiesRequest = UpdateNamespacePropertiesRequest.builder()
                        .updateAll(updates)
                        .removeAll(removals).build();
                icebergTableOps.updateNamespaceProperties(icebergSchema.getIcebergNamespace(), updateNamespacePropertiesRequest);
                return icebergSchema;
              });

      LOG.info("Altered Iceberg schema (database) {} in IcebergMetastore", ident.name());
      return alteredIcebergSchema;
    } catch (NoSuchNamespaceException e) {
      throw new NoSuchSchemaException(
          String.format("Iceberg schema (database) %s does not exist in IcebergMetastore", ident.name()),
          e);

    } catch (EntityAlreadyExistsException e) {
      throw new NoSuchSchemaException(
          "The new Iceberg schema (database) name already exist in Graviton store", e);

    } catch (IOException e) {
      throw new RuntimeException(
          "Failed to alter Iceberg schema (database) " + ident.name() + " in Graviton store", e);

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
      LOG.error("Cannot support invalid namespace in IcebergMetastore: {}", ident.namespace());
      return false;
    }
    EntityStore store = GravitonEnv.getInstance().entityStore();
    Namespace schemaNamespace =
        Namespace.of(Stream.of(ident.namespace().levels(), ident.name()).toArray(String[]::new));
    List<BaseTable> tables = Lists.newArrayList();
    if (!cascade) {
      if (listTables(schemaNamespace).length > 0) {
        throw new NonEmptySchemaException(
            String.format(
                "Iceberg schema (database) %s is not empty. One or more tables exist in Iceberg metastore.",
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
                "Iceberg schema (database) %s is not empty. One or more tables exist in Graviton store.",
                ident.name()));
      }
    }

    try {
      store.executeInTransaction(
          () -> {
            for (BaseTable t : tables) {
              store.delete(NameIdentifier.of(schemaNamespace, t.name()), TABLE);
            }
            store.delete(ident, SCHEMA, true);
            icebergTableOps.dropNamespace(org.apache.iceberg.catalog.Namespace.of(ident.toString()));
            return null;
          });

      LOG.info("Dropped Iceberg schema (database) {}", ident.name());
      return true;
    } catch (NamespaceNotEmptyException e) {
      throw new NonEmptySchemaException(
          String.format(
              "Iceberg schema (database) %s is not empty. One or more tables exist.", ident.name()),
          e);

    } catch (NoSuchNamespaceException e) {
      deleteSchemaFromStore(ident);
      LOG.warn("Iceberg schema (database) {} does not exist in IcebergMetastore", ident.name());
      return false;

    } catch (IOException e) {
      throw new RuntimeException(
          "Failed to drop Iceberg schema (database) " + ident.name() + " in Graviton store", e);

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void deleteSchemaFromStore(NameIdentifier ident) {
    EntityStore store = GravitonEnv.getInstance().entityStore();
    try {
      store.delete(ident, SCHEMA);
    } catch (IOException ex) {
      LOG.error("Failed to delete Iceberg schema {} from Graviton store", ident, ex);
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
    //TODO supported method.
    throw new UnsupportedOperationException();
  }

  /**
   * Loads a table from the IcebergMetastore.
   *
   * @param tableIdent The identifier of the table to load.
   * @return The loaded IcebergTable instance representing the table.
   * @throws NoSuchTableException If the specified table does not exist in the IcebergMetastore.
   */
  @Override
  public Table loadTable(NameIdentifier tableIdent) throws NoSuchTableException {
    //TODO supported method.
    throw new UnsupportedOperationException();
  }

  /**
   * Creates a new table in the IcebergMetastore.
   *
   * @param tableIdent The identifier of the table to create.
   * @param columns The array of columns for the new table.
   * @param comment The comment for the new table.
   * @param properties The properties for the new table.
   * @return The newly created IcebergTable instance.
   * @throws NoSuchSchemaException If the schema for the table does not exist.
   * @throws TableAlreadyExistsException If the table with the same name already exists.
   */
  @Override
  public Table createTable(
      NameIdentifier tableIdent, Column[] columns, String comment, Map<String, String> properties)
      throws NoSuchSchemaException, TableAlreadyExistsException {
    //TODO supported method.
    throw new UnsupportedOperationException();
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
    //TODO supported method.
    throw new UnsupportedOperationException();
  }

  /**
   * Drops a table from the IcebergMetastore.
   *
   * @param tableIdent The identifier of the table to drop.
   * @return true if the table is successfully dropped; false if the table does not exist.
   */
  @Override
  public boolean dropTable(NameIdentifier tableIdent) {
    //TODO supported method.
    throw new UnsupportedOperationException();
  }

  /**
   * Purges a table from the IcebergMetastore.
   *
   * @param tableIdent The identifier of the table to purge.
   * @return true if the table is successfully purged; false if the table does not exist.
   * @throws UnsupportedOperationException If the table type is EXTERNAL_TABLE, it cannot be purged.
   */
  @Override
  public boolean purgeTable(NameIdentifier tableIdent) throws UnsupportedOperationException {
    //TODO supported method.
    throw new UnsupportedOperationException();
  }

  /**
   * Checks if the given namespace is a valid namespace for the Icebergschema.
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
//    try {
//      username = UserGroupInformation.getCurrentUser().getShortUserName();
//    } catch (IOException e) {
//      LOG.warn("Failed to get Hadoop user", e);
//    }

    if (username != null) {
      return username;
    } else {
      LOG.warn("Hadoop user is null, defaulting to user.name");
      return System.getProperty("user.name");
    }
  }
}
