/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.catalog.lakehouse.iceberg;

import com.datastrato.graviton.NameIdentifier;
import com.datastrato.graviton.Namespace;
import com.datastrato.graviton.catalog.CatalogOperations;
import com.datastrato.graviton.catalog.lakehouse.iceberg.ops.IcebergTableOps;
import com.datastrato.graviton.catalog.lakehouse.iceberg.ops.IcebergTableOpsHelper;
import com.datastrato.graviton.exceptions.NoSuchCatalogException;
import com.datastrato.graviton.exceptions.NoSuchSchemaException;
import com.datastrato.graviton.exceptions.NoSuchTableException;
import com.datastrato.graviton.exceptions.NonEmptySchemaException;
import com.datastrato.graviton.exceptions.SchemaAlreadyExistsException;
import com.datastrato.graviton.exceptions.TableAlreadyExistsException;
import com.datastrato.graviton.meta.AuditInfo;
import com.datastrato.graviton.meta.CatalogEntity;
import com.datastrato.graviton.rel.Column;
import com.datastrato.graviton.rel.Distribution;
import com.datastrato.graviton.rel.SchemaChange;
import com.datastrato.graviton.rel.SortOrder;
import com.datastrato.graviton.rel.SupportsSchemas;
import com.datastrato.graviton.rel.Table;
import com.datastrato.graviton.rel.TableCatalog;
import com.datastrato.graviton.rel.TableChange;
import com.datastrato.graviton.rel.transforms.Transform;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.rest.requests.UpdateNamespacePropertiesRequest;
import org.apache.iceberg.rest.responses.GetNamespaceResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Operations for interacting with the Iceberg catalog in Graviton. */
public class IcebergCatalogOperations implements CatalogOperations, SupportsSchemas, TableCatalog {

  public static final Logger LOG = LoggerFactory.getLogger(IcebergCatalogOperations.class);

  @VisibleForTesting IcebergTableOps icebergTableOps;

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
    IcebergRESTConfig icebergConfig = new IcebergRESTConfig();
    icebergConfig.loadFromMap(conf, k -> true);
    this.icebergTableOps = new IcebergTableOps(icebergConfig);
  }

  /** Closes the Iceberg catalog and releases the associated client pool. */
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
    try {
      return icebergTableOps
          .listNamespace(IcebergTableOpsHelper.getIcebergNamespace(namespace.levels())).namespaces()
          .stream()
          .map(icebergNamespace -> NameIdentifier.of(icebergNamespace.levels()))
          .toArray(NameIdentifier[]::new);
    } catch (NoSuchNamespaceException e) {
      throw new NoSuchSchemaException(
          "Failed to list all schemas (database) under namespace : " + namespace + " in Iceberg",
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
  public IcebergSchema createSchema(
      NameIdentifier ident, String comment, Map<String, String> metadata)
      throws NoSuchCatalogException, SchemaAlreadyExistsException {
    try {
      String currentUser = currentUser();
      IcebergSchema createdSchema =
          new IcebergSchema.Builder()
              .withName(ident.name())
              .withComment(comment)
              .withProperties(metadata)
              .withAuditInfo(
                  new AuditInfo.Builder()
                      .withCreator(currentUser)
                      .withCreateTime(Instant.now())
                      .build())
              .build();
      icebergTableOps.createNamespace(
          createdSchema.toCreateRequest(IcebergTableOpsHelper.getIcebergNamespace(ident)));
      LOG.info(
          "Created Iceberg schema (database) {} in Iceberg\ncurrentUser:{} \ncomment: {} \nmetadata: {}",
          ident.name(),
          currentUser,
          comment,
          metadata);
      return createdSchema;
    } catch (org.apache.iceberg.exceptions.AlreadyExistsException e) {
      throw new SchemaAlreadyExistsException(
          String.format("Iceberg schema (database) '%s' already exists in Iceberg", ident.name()),
          e);
    } catch (NoSuchNamespaceException e) {
      throw new NoSuchSchemaException(
          String.format(
              "Iceberg schema (database) does not exist: %s in Graviton store, This scenario occurs after the creation is completed and reloaded",
              ident.name()),
          e);
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
    try {
      GetNamespaceResponse response =
          icebergTableOps.loadNamespace(IcebergTableOpsHelper.getIcebergNamespace(ident));
      IcebergSchema icebergSchema =
          new IcebergSchema.Builder()
              .withName(ident.name())
              .withComment(
                  Optional.of(response)
                      .map(GetNamespaceResponse::properties)
                      .map(map -> map.get(IcebergSchema.ICEBERG_COMMENT_FIELD_NAME))
                      .orElse(""))
              .withProperties(response.properties())
              .build();
      LOG.info("Loaded Iceberg schema (database) {} from Iceberg ", ident.name());
      return icebergSchema;
    } catch (NoSuchNamespaceException e) {
      throw new NoSuchSchemaException(
          String.format(
              "Iceberg schema (database) does not exist: %s in Graviton store", ident.name()),
          e);
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
    try {
      GetNamespaceResponse response =
          icebergTableOps.loadNamespace(IcebergTableOpsHelper.getIcebergNamespace(ident));
      Map<String, String> metadata = response.properties();
      List<String> removals = new ArrayList<>();
      Map<String, String> updates = new HashMap<>();
      Map<String, String> resultProperties = new HashMap<>(metadata);
      for (SchemaChange change : changes) {
        if (change instanceof SchemaChange.SetProperty) {
          String key = ((SchemaChange.SetProperty) change).getProperty();
          String val = ((SchemaChange.SetProperty) change).getValue();
          updates.put(key, val);
          resultProperties.put(key, val);
        } else if (change instanceof SchemaChange.RemoveProperty) {
          removals.add(((SchemaChange.RemoveProperty) change).getProperty());
          resultProperties.remove(((SchemaChange.RemoveProperty) change).getProperty());
        } else {
          throw new IllegalArgumentException(
              "Unsupported schema change type: " + change.getClass().getSimpleName());
        }
      }

      // Determine whether to change comment.
      String comment;
      if (removals.contains(IcebergSchema.ICEBERG_COMMENT_FIELD_NAME)) {
        comment = null;
      } else if (updates.containsKey(IcebergSchema.ICEBERG_COMMENT_FIELD_NAME)) {
        comment = updates.get(IcebergSchema.ICEBERG_COMMENT_FIELD_NAME);
      } else {
        // Synchronize changes to underlying iceberg sources
        comment =
            Optional.of(response.properties())
                .map(map -> map.get(IcebergSchema.ICEBERG_COMMENT_FIELD_NAME))
                .orElse(null);
      }
      IcebergSchema icebergSchema =
          new IcebergSchema.Builder()
              .withName(ident.name())
              .withComment(comment)
              .withProperties(resultProperties)
              .build();
      UpdateNamespacePropertiesRequest updateNamespacePropertiesRequest =
          UpdateNamespacePropertiesRequest.builder().updateAll(updates).removeAll(removals).build();
      icebergTableOps.updateNamespaceProperties(
          IcebergTableOpsHelper.getIcebergNamespace(ident), updateNamespacePropertiesRequest);
      LOG.info("Altered Iceberg schema (database) {} in Iceberg", ident.name());
      return icebergSchema;
    } catch (NoSuchNamespaceException e) {
      throw new NoSuchSchemaException(
          String.format("Iceberg schema (database) %s does not exist in Iceberg", ident.name()), e);
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
    Preconditions.checkArgument(!cascade, "Iceberg does not support cascading delete operations.");
    try {
      icebergTableOps.dropNamespace(IcebergTableOpsHelper.getIcebergNamespace(ident));
      LOG.info("Dropped Iceberg schema (database) {}", ident.name());
      return true;
    } catch (NamespaceNotEmptyException e) {
      throw new NonEmptySchemaException(
          String.format(
              "Iceberg schema (database) %s is not empty. One or more tables exist.", ident.name()),
          e);
    } catch (NoSuchNamespaceException e) {
      LOG.warn("Iceberg schema (database) {} does not exist in Iceberg", ident.name());
      return false;
    } catch (Exception e) {
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
    // TODO supported method.
    throw new UnsupportedOperationException();
  }

  /**
   * Loads a table from the Iceberg.
   *
   * @param tableIdent The identifier of the table to load.
   * @return The loaded IcebergTable instance representing the table.
   * @throws NoSuchTableException If the specified table does not exist in the Iceberg.
   */
  @Override
  public Table loadTable(NameIdentifier tableIdent) throws NoSuchTableException {
    // TODO supported method.
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean tableExists(NameIdentifier ident) {
    return TableCatalog.super.tableExists(ident);
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
    // TODO supported method.
    throw new UnsupportedOperationException();
  }

  /**
   * Drops a table from the Iceberg.
   *
   * @param tableIdent The identifier of the table to drop.
   * @return true if the table is successfully dropped; false if the table does not exist.
   */
  @Override
  public boolean dropTable(NameIdentifier tableIdent) {
    // TODO supported method.
    throw new UnsupportedOperationException();
  }

  /**
   * Creates a new table in the Iceberg.
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
      Transform[] partitions,
      Distribution distribution,
      SortOrder[] sortOrders)
      throws NoSuchSchemaException, TableAlreadyExistsException {
    throw new UnsupportedOperationException();
  }

  /**
   * Purges a table from the Iceberg.
   *
   * @param tableIdent The identifier of the table to purge.
   * @return true if the table is successfully purged; false if the table does not exist.
   * @throws UnsupportedOperationException If the table type is EXTERNAL_TABLE, it cannot be purged.
   */
  @Override
  public boolean purgeTable(NameIdentifier tableIdent) throws UnsupportedOperationException {
    // TODO supported method.
    throw new UnsupportedOperationException();
  }

  // TODO. We should figure out a better way to get the current user from servlet container.
  private static String currentUser() {
    return System.getProperty("user.name");
  }
}
