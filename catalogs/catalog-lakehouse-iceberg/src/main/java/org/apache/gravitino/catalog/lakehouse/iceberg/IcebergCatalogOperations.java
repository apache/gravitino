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
package org.apache.gravitino.catalog.lakehouse.iceberg;

import static org.apache.gravitino.connector.BaseCatalog.CATALOG_BYPASS_PREFIX;

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
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.catalog.lakehouse.iceberg.ops.IcebergCatalogWrapperHelper;
import org.apache.gravitino.connector.CatalogInfo;
import org.apache.gravitino.connector.CatalogOperations;
import org.apache.gravitino.connector.HasPropertyMetadata;
import org.apache.gravitino.connector.SupportsSchemas;
import org.apache.gravitino.exceptions.ConnectionFailedException;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.NonEmptySchemaException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.common.ops.IcebergCatalogWrapper;
import org.apache.gravitino.iceberg.common.ops.IcebergCatalogWrapper.IcebergTableChange;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.utils.MapUtils;
import org.apache.gravitino.utils.PrincipalUtils;
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

/** Operations for interacting with an Apache Iceberg catalog in Apache Gravitino. */
public class IcebergCatalogOperations implements CatalogOperations, SupportsSchemas, TableCatalog {

  private static final String ICEBERG_TABLE_DOES_NOT_EXIST_MSG = "Iceberg table does not exist: %s";

  public static final Logger LOG = LoggerFactory.getLogger(IcebergCatalogOperations.class);

  @VisibleForTesting IcebergCatalogWrapper icebergCatalogWrapper;

  private IcebergCatalogWrapperHelper icebergCatalogWrapperHelper;

  /**
   * Initializes the Iceberg catalog operations with the provided configuration.
   *
   * @param conf The configuration map for the Iceberg catalog operations.
   * @param info The catalog info associated with this operations instance.
   * @throws RuntimeException if initialization fails.
   */
  @Override
  public void initialize(
      Map<String, String> conf, CatalogInfo info, HasPropertyMetadata propertiesMetadata)
      throws RuntimeException {
    // Key format like gravitino.bypass.a.b
    Map<String, String> prefixMap = MapUtils.getPrefixMap(conf, CATALOG_BYPASS_PREFIX);

    // Hold keys that lie in GRAVITINO_CONFIG_TO_ICEBERG
    Map<String, String> gravitinoConfig =
        ((IcebergCatalogPropertiesMetadata) propertiesMetadata.catalogPropertiesMetadata())
            .transformProperties(conf);

    Map<String, String> resultConf = Maps.newHashMap(prefixMap);
    resultConf.putAll(gravitinoConfig);
    resultConf.put("catalog_uuid", info.id().toString());
    IcebergConfig icebergConfig = new IcebergConfig(resultConf);

    this.icebergCatalogWrapper = new IcebergCatalogWrapper(icebergConfig);
    this.icebergCatalogWrapperHelper =
        new IcebergCatalogWrapperHelper(icebergCatalogWrapper.getCatalog());
  }

  /** Closes the Iceberg catalog and releases the associated client pool. */
  @Override
  public void close() {
    if (null != icebergCatalogWrapper) {
      try {
        icebergCatalogWrapper.close();
      } catch (Exception e) {
        LOG.warn("Failed to close Iceberg catalog", e);
      }
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
    try {
      List<org.apache.iceberg.catalog.Namespace> namespaces =
          icebergCatalogWrapper
              .listNamespace(IcebergCatalogWrapperHelper.getIcebergNamespace())
              .namespaces();

      return namespaces.stream()
          .map(icebergNamespace -> NameIdentifier.of(namespace, icebergNamespace.toString()))
          .toArray(NameIdentifier[]::new);
    } catch (NoSuchNamespaceException e) {
      throw new NoSuchSchemaException(
          e, "Failed to list all schemas (database) under namespace : %s in Iceberg", namespace);
    }
  }

  /**
   * Creates a new schema with the provided identifier, comment, and metadata.
   *
   * @param ident The identifier of the schema to create.
   * @param comment The comment for the schema.
   * @param properties The properties for the schema.
   * @return The created {@link IcebergSchema}.
   * @throws NoSuchCatalogException If the provided namespace is invalid or does not exist.
   * @throws SchemaAlreadyExistsException If a schema with the same name already exists.
   */
  @Override
  public IcebergSchema createSchema(
      NameIdentifier ident, String comment, Map<String, String> properties)
      throws NoSuchCatalogException, SchemaAlreadyExistsException {
    try {
      String currentUser = currentUser();
      IcebergSchema createdSchema =
          IcebergSchema.builder()
              .withName(ident.name())
              .withComment(comment)
              .withProperties(properties)
              .withAuditInfo(
                  AuditInfo.builder()
                      .withCreator(currentUser)
                      .withCreateTime(Instant.now())
                      .build())
              .build();
      icebergCatalogWrapper.createNamespace(
          createdSchema.toCreateRequest(
              IcebergCatalogWrapperHelper.getIcebergNamespace(ident.name())));
      LOG.info(
          "Created Iceberg schema (database) {} in Iceberg\ncurrentUser:{} \ncomment: {} \nmetadata: {}",
          ident.name(),
          currentUser,
          comment,
          properties);
      return createdSchema;
    } catch (org.apache.iceberg.exceptions.AlreadyExistsException e) {
      throw new SchemaAlreadyExistsException(
          e, "Iceberg schema (database) '%s' already exists", ident.name());
    } catch (NoSuchNamespaceException e) {
      throw new NoSuchSchemaException(
          e,
          "Iceberg schema (database) does not exist: %s in Gravitino store, This scenario occurs after the creation is completed and reloaded",
          ident.name());
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
          icebergCatalogWrapper.loadNamespace(
              IcebergCatalogWrapperHelper.getIcebergNamespace(ident.name()));
      IcebergSchema icebergSchema =
          IcebergSchema.builder()
              .withName(ident.name())
              .withComment(
                  Optional.of(response)
                      .map(GetNamespaceResponse::properties)
                      .map(map -> map.get(IcebergSchemaPropertiesMetadata.COMMENT))
                      .orElse(null))
              .withProperties(response.properties())
              .withAuditInfo(AuditInfo.EMPTY)
              .build();
      LOG.info("Loaded Iceberg schema (database) {} from Iceberg ", ident.name());
      return icebergSchema;
    } catch (NoSuchNamespaceException e) {
      throw new NoSuchSchemaException(
          e, "Iceberg schema (database) does not exist: %s in Gravitino store", ident.name());
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
          icebergCatalogWrapper.loadNamespace(
              IcebergCatalogWrapperHelper.getIcebergNamespace(ident.name()));
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

      String comment =
          Optional.of(response.properties())
              .map(map -> map.get(IcebergSchemaPropertiesMetadata.COMMENT))
              .orElse(null);
      IcebergSchema icebergSchema =
          IcebergSchema.builder()
              .withName(ident.name())
              .withComment(comment)
              .withAuditInfo(AuditInfo.EMPTY)
              .withProperties(resultProperties)
              .build();
      UpdateNamespacePropertiesRequest updateNamespacePropertiesRequest =
          UpdateNamespacePropertiesRequest.builder().updateAll(updates).removeAll(removals).build();
      UpdateNamespacePropertiesResponse updateNamespacePropertiesResponse =
          icebergCatalogWrapper.updateNamespaceProperties(
              IcebergCatalogWrapperHelper.getIcebergNamespace(ident.name()),
              updateNamespacePropertiesRequest);
      LOG.info(
          "Altered Iceberg schema (database) {}. UpdateResponse:\n{}",
          ident.name(),
          updateNamespacePropertiesResponse);
      return icebergSchema;
    } catch (NoSuchNamespaceException e) {
      throw new NoSuchSchemaException(
          e, "Iceberg schema (database) %s does not exist", ident.name());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Drops the schema with the provided identifier.
   *
   * @param ident The identifier of the schema to drop.
   * @param cascade If set to true, drops all the tables in the schema as well.
   * @return true if the schema was dropped successfully, false if the schema does not exist.
   * @throws NonEmptySchemaException If the schema is not empty and 'cascade' is set to false.
   */
  @Override
  public boolean dropSchema(NameIdentifier ident, boolean cascade) throws NonEmptySchemaException {
    Preconditions.checkArgument(!cascade, "Iceberg does not support cascading delete operations.");
    try {
      icebergCatalogWrapper.dropNamespace(
          IcebergCatalogWrapperHelper.getIcebergNamespace(ident.name()));
      LOG.info("Dropped Iceberg schema (database) {}", ident.name());
      return true;
    } catch (NamespaceNotEmptyException e) {
      throw new NonEmptySchemaException(
          e, "Iceberg schema (database) %s is not empty. One or more tables exist.", ident.name());
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
    NameIdentifier schemaIdent = NameIdentifier.of(namespace.levels());
    if (!schemaExists(schemaIdent)) {
      throw new NoSuchSchemaException("Schema (database) does not exist %s", namespace);
    }

    try {
      ListTablesResponse listTablesResponse =
          icebergCatalogWrapper.listTable(
              IcebergCatalogWrapperHelper.getIcebergNamespace(namespace));
      return listTablesResponse.identifiers().stream()
          .map(
              tableIdentifier ->
                  NameIdentifier.of(ArrayUtils.add(namespace.levels(), tableIdentifier.name())))
          .toArray(NameIdentifier[]::new);
    } catch (NoSuchNamespaceException e) {
      throw new NoSuchSchemaException("Schema (database) does not exist %s in Iceberg", namespace);
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
          icebergCatalogWrapper.loadTable(
              IcebergCatalogWrapperHelper.buildIcebergTableIdentifier(tableIdent));
      IcebergTable icebergTable =
          IcebergTable.fromIcebergTable(tableResponse.tableMetadata(), tableIdent.name());

      LOG.info("Loaded Iceberg table {}", tableIdent.name());
      return icebergTable;
    } catch (org.apache.iceberg.exceptions.NoSuchTableException e) {
      throw new NoSuchTableException(e, ICEBERG_TABLE_DOES_NOT_EXIST_MSG, tableIdent.name());
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
      String[] levels = tableIdent.namespace().levels();
      IcebergTableChange icebergTableChange =
          icebergCatalogWrapperHelper.buildIcebergTableChanges(
              NameIdentifier.of(levels[levels.length - 1], tableIdent.name()), changes);
      LoadTableResponse loadTableResponse = icebergCatalogWrapper.updateTable(icebergTableChange);
      loadTableResponse.validate();
      return IcebergTable.fromIcebergTable(loadTableResponse.tableMetadata(), tableIdent.name());
    } catch (org.apache.iceberg.exceptions.NoSuchTableException e) {
      throw new NoSuchTableException(e, ICEBERG_TABLE_DOES_NOT_EXIST_MSG, tableIdent.name());
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
              .withSource(IcebergCatalogWrapperHelper.buildIcebergTableIdentifier(tableIdent))
              .withDestination(
                  IcebergCatalogWrapperHelper.buildIcebergTableIdentifier(
                      tableIdent.namespace(), renameTable.getNewName()))
              .build();
      icebergCatalogWrapper.renameTable(renameTableRequest);
      return loadTable(NameIdentifier.of(tableIdent.namespace(), renameTable.getNewName()));
    } catch (org.apache.iceberg.exceptions.NoSuchTableException e) {
      throw new NoSuchTableException(e, ICEBERG_TABLE_DOES_NOT_EXIST_MSG, tableIdent.name());
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
      icebergCatalogWrapper.dropTable(
          IcebergCatalogWrapperHelper.buildIcebergTableIdentifier(tableIdent));
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
   * @param partitioning The partitioning for the new table.
   * @param indexes The indexes for the new table.
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
      Transform[] partitioning,
      Distribution distribution,
      SortOrder[] sortOrders,
      Index[] indexes)
      throws NoSuchSchemaException, TableAlreadyExistsException {
    Preconditions.checkArgument(indexes.length == 0, "Iceberg-catalog does not support indexes");
    try {
      NameIdentifier schemaIdent = NameIdentifier.of(tableIdent.namespace().levels());
      if (!schemaExists(schemaIdent)) {
        LOG.warn("Iceberg schema (database) does not exist: {}", schemaIdent);
        throw new NoSuchSchemaException("Iceberg Schema (database) does not exist %s", schemaIdent);
      }
      IcebergColumn[] icebergColumns =
          Arrays.stream(columns)
              .map(
                  column ->
                      IcebergColumn.builder()
                          .withName(column.name())
                          .withType(column.dataType())
                          .withComment(column.comment())
                          .withNullable(column.nullable())
                          .withAuditInfo(AuditInfo.EMPTY)
                          .build())
              .toArray(IcebergColumn[]::new);

      // Gravitino NONE distribution means the client side doesn't specify distribution, which is
      // not the same as none distribution in Iceberg.
      if (Distributions.NONE.equals(distribution)) {
        distribution =
            getIcebergDefaultDistribution(sortOrders.length > 0, partitioning.length > 0);
      }

      IcebergTable createdTable =
          IcebergTable.builder()
              .withName(tableIdent.name())
              .withColumns(icebergColumns)
              .withComment(comment)
              .withPartitioning(partitioning)
              .withSortOrders(sortOrders)
              .withProperties(properties)
              .withDistribution(distribution)
              .withAuditInfo(
                  AuditInfo.builder()
                      .withCreator(currentUser())
                      .withCreateTime(Instant.now())
                      .build())
              .build();

      LoadTableResponse loadTableResponse =
          icebergCatalogWrapper.createTable(
              IcebergCatalogWrapperHelper.getIcebergNamespace(schemaIdent.name()),
              createdTable.toCreateTableRequest());
      loadTableResponse.validate();

      LOG.info("Created Iceberg table {}", tableIdent.name());
      return createdTable;
    } catch (AlreadyExistsException e) {
      throw new TableAlreadyExistsException(e, "Table already exists: %s", tableIdent.name());
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
      String schema = NameIdentifier.of(tableIdent.namespace().levels()).name();
      icebergCatalogWrapper.purgeTable(TableIdentifier.of(schema, tableIdent.name()));
      LOG.info("Purge Iceberg table {}", tableIdent.name());
      return true;
    } catch (org.apache.iceberg.exceptions.NoSuchTableException e) {
      LOG.warn("Iceberg table {} does not exist", tableIdent.name());
      return false;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Performs `listNamespaces` operation on the Iceberg catalog to test the connection.
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
    try {
      icebergCatalogWrapper.listNamespace(IcebergCatalogWrapperHelper.getIcebergNamespace());
    } catch (Exception e) {
      throw new ConnectionFailedException(
          e, "Failed to run listNamespace on Iceberg catalog: %s", e.getMessage());
    }
  }

  private static Distribution getIcebergDefaultDistribution(
      boolean isSorted, boolean isPartitioned) {
    if (isSorted) {
      return Distributions.RANGE;
    } else if (isPartitioned) {
      return Distributions.HASH;
    }
    return Distributions.NONE;
  }

  private static String currentUser() {
    return PrincipalUtils.getCurrentUserName();
  }
}
