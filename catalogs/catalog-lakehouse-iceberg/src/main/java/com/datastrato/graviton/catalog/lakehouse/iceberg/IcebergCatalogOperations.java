/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.catalog.lakehouse.iceberg;

import static com.datastrato.graviton.catalog.BaseCatalog.CATALOG_BYPASS_PREFIX;

import com.datastrato.graviton.NameIdentifier;
import com.datastrato.graviton.Namespace;
import com.datastrato.graviton.catalog.CatalogOperations;
import com.datastrato.graviton.catalog.PropertiesMetadata;
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
import com.datastrato.graviton.utils.MapUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.rest.requests.RenameTableRequest;
import org.apache.iceberg.rest.requests.UpdateNamespacePropertiesRequest;
import org.apache.iceberg.rest.responses.GetNamespaceResponse;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.UpdateNamespacePropertiesResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Operations for interacting with the Iceberg catalog in Graviton. */
public class IcebergCatalogOperations implements CatalogOperations, SupportsSchemas, TableCatalog {

  public static final Logger LOG = LoggerFactory.getLogger(IcebergCatalogOperations.class);

  @VisibleForTesting IcebergTableOps icebergTableOps;

  private IcebergCatalogPropertiesMetadata icebergCatalogPropertiesMetadata;

  private IcebergTablePropertiesMetadata icebergTablePropertiesMetadata;

  private final CatalogEntity entity;

  private IcebergTableOpsHelper icebergTableOpsHelper;

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
    // Key format like graviton.bypass.a.b
    Map<String, String> prefixMap = MapUtils.getPrefixMap(conf, CATALOG_BYPASS_PREFIX);

    this.icebergCatalogPropertiesMetadata = new IcebergCatalogPropertiesMetadata();
    // Hold keys that lie in GRAVITON_CONFIG_TO_ICEBERG
    Map<String, String> gravitonConfig =
        this.icebergCatalogPropertiesMetadata.transformProperties(conf);

    Map<String, String> resultConf = Maps.newHashMap(prefixMap);
    resultConf.putAll(gravitonConfig);

    IcebergConfig icebergConfig = new IcebergConfig();
    icebergConfig.loadFromMap(resultConf, k -> true);

    this.icebergTableOps = new IcebergTableOps(icebergConfig);
    this.icebergTableOpsHelper = icebergTableOps.createIcebergTableOpsHelper();
    this.icebergTablePropertiesMetadata = new IcebergTablePropertiesMetadata();
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
          String.format("Iceberg schema (database) '%s' already exists", ident.name()), e);
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
              .withAuditInfo(AuditInfo.EMPTY)
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
              .withAuditInfo(AuditInfo.EMPTY)
              .withProperties(resultProperties)
              .build();
      UpdateNamespacePropertiesRequest updateNamespacePropertiesRequest =
          UpdateNamespacePropertiesRequest.builder().updateAll(updates).removeAll(removals).build();
      UpdateNamespacePropertiesResponse updateNamespacePropertiesResponse =
          icebergTableOps.updateNamespaceProperties(
              IcebergTableOpsHelper.getIcebergNamespace(ident), updateNamespacePropertiesRequest);
      LOG.info(
          "Altered Iceberg schema (database) {}. UpdateResponse:\n{}",
          ident.name(),
          updateNamespacePropertiesResponse);
      return icebergSchema;
    } catch (NoSuchNamespaceException e) {
      throw new NoSuchSchemaException(
          String.format("Iceberg schema (database) %s does not exist", ident.name()), e);
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
      LOG.warn("Iceberg schema (database) {} does not exist", ident.name());
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
    try {
      ListTablesResponse listTablesResponse =
          icebergTableOps.listTable(IcebergTableOpsHelper.getIcebergNamespace(namespace.levels()));
      return listTablesResponse.identifiers().stream()
          .map(
              tableIdentifier ->
                  NameIdentifier.of(
                      ArrayUtils.add(tableIdentifier.namespace().levels(), tableIdentifier.name())))
          .toArray(NameIdentifier[]::new);
    } catch (NoSuchNamespaceException e) {
      throw new NoSuchSchemaException(
          "Schema (database) does not exist " + namespace + " in Iceberg");
    }
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
    try {

      LoadTableResponse tableResponse =
          icebergTableOps.loadTable(IcebergTableOpsHelper.buildIcebergTableIdentifier(tableIdent));
      IcebergTable icebergTable =
          IcebergTable.fromIcebergTable(tableResponse.tableMetadata(), tableIdent.name());

      LOG.info("Loaded Iceberg table {}", tableIdent.name());
      return icebergTable;
    } catch (org.apache.iceberg.exceptions.NoSuchTableException e) {
      throw new NoSuchTableException(
          String.format("Iceberg table does not exist: %s", tableIdent.name()), e);
    }
  }

  /**
   * Apply the {@link TableChange change} to an existing Iceberg table.
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
    return internalUpdateTable(tableIdent, changes);
  }

  private Table internalUpdateTable(NameIdentifier tableIdent, TableChange... changes)
      throws NoSuchTableException, IllegalArgumentException {
    try {
      IcebergTableOpsHelper.IcebergTableChange icebergTableChange =
          icebergTableOpsHelper.buildIcebergTableChanges(tableIdent, changes);
      LoadTableResponse loadTableResponse = icebergTableOps.updateTable(icebergTableChange);
      loadTableResponse.validate();
      return IcebergTable.fromIcebergTable(loadTableResponse.tableMetadata(), tableIdent.name());
    } catch (org.apache.iceberg.exceptions.NoSuchTableException e) {
      throw new NoSuchTableException(
          String.format("Iceberg table does not exist: %s", tableIdent.name()), e);
    }
  }

  /**
   * Perform name change operations on the Iceberg.
   *
   * @param tableIdent tableIdent of this table.
   * @param renameTable Table Change to modify the table name.
   * @return Returns the table for Iceberg.
   * @throws NoSuchTableException
   * @throws IllegalArgumentException
   */
  private Table renameTable(NameIdentifier tableIdent, TableChange.RenameTable renameTable)
      throws NoSuchTableException, IllegalArgumentException {
    try {
      RenameTableRequest renameTableRequest =
          RenameTableRequest.builder()
              .withSource(IcebergTableOpsHelper.buildIcebergTableIdentifier(tableIdent))
              .withDestination(
                  IcebergTableOpsHelper.buildIcebergTableIdentifier(
                      tableIdent.namespace(), renameTable.getNewName()))
              .build();
      icebergTableOps.renameTable(renameTableRequest);
      return loadTable(NameIdentifier.of(tableIdent.namespace(), renameTable.getNewName()));
    } catch (org.apache.iceberg.exceptions.NoSuchTableException e) {
      throw new NoSuchTableException(
          String.format("Iceberg table does not exist: %s", tableIdent.name()), e);
    }
  }

  /**
   * Drops a table from the Iceberg.
   *
   * @param tableIdent The identifier of the table to drop.
   * @return true if the table is successfully dropped; false if the table does not exist.
   */
  @Override
  public boolean dropTable(NameIdentifier tableIdent) {
    try {
      icebergTableOps.dropTable(
          TableIdentifier.of(ArrayUtils.add(tableIdent.namespace().levels(), tableIdent.name())));
      LOG.info("Dropped Iceberg table {}", tableIdent.name());
      return true;
    } catch (org.apache.iceberg.exceptions.NoSuchTableException e) {
      LOG.warn("Iceberg table {} does not exist", tableIdent.name());
      return false;
    }
  }

  /**
   * Creates a new table in the Iceberg.
   *
   * @param tableIdent The identifier of the table to create.
   * @param columns The array of columns for the new table.
   * @param comment The comment for the new table.
   * @param properties The properties for the new table.
   * @param partitions The partitioning for the new table.
   * @return The newly created IcebergTable instance.
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
    try {
      NameIdentifier schemaIdent = NameIdentifier.of(tableIdent.namespace().levels());
      if (!schemaExists(schemaIdent)) {
        LOG.warn("Iceberg schema (database) does not exist: {}", schemaIdent);
        throw new NoSuchSchemaException("Iceberg Schema (database) does not exist " + schemaIdent);
      }

      IcebergTable createdTable =
          new IcebergTable.Builder()
              .withName(tableIdent.name())
              .withColumns(columns)
              .withComment(comment)
              .withPartitions(partitions)
              .withSortOrders(sortOrders)
              .withProperties(properties)
              .withAuditInfo(
                  new AuditInfo.Builder()
                      .withCreator(currentUser())
                      .withCreateTime(Instant.now())
                      .build())
              .build();

      LoadTableResponse loadTableResponse =
          icebergTableOps.createTable(
              IcebergTableOpsHelper.getIcebergNamespace(tableIdent.namespace().levels()),
              createdTable.toCreateTableRequest());
      loadTableResponse.validate();

      LOG.info("Created Iceberg table {}", tableIdent.name());
      return createdTable;
    } catch (AlreadyExistsException e) {
      throw new TableAlreadyExistsException("Table already exists: " + tableIdent.name(), e);
    }
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
    try {
      icebergTableOps.purgeTable(
          TableIdentifier.of(ArrayUtils.add(tableIdent.namespace().levels(), tableIdent.name())));
      LOG.info("Purge Iceberg table {}", tableIdent.name());
      return true;
    } catch (org.apache.iceberg.exceptions.NoSuchTableException e) {
      LOG.warn("Iceberg table {} does not exist", tableIdent.name());
      return false;
    }
  }

  // TODO. We should figure out a better way to get the current user from servlet container.
  private static String currentUser() {
    return System.getProperty("user.name");
  }

  @Override
  public PropertiesMetadata tablePropertiesMetadata() throws UnsupportedOperationException {
    return icebergTablePropertiesMetadata;
  }

  @Override
  public PropertiesMetadata catalogPropertiesMetadata() throws UnsupportedOperationException {
    return icebergCatalogPropertiesMetadata;
  }
}
