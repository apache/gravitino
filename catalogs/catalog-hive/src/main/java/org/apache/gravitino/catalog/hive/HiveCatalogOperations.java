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
package org.apache.gravitino.catalog.hive;

import static org.apache.gravitino.catalog.hive.HiveCatalogPropertiesMetadata.LIST_ALL_TABLES;
import static org.apache.gravitino.catalog.hive.HiveCatalogPropertiesMetadata.METASTORE_URIS;
import static org.apache.gravitino.catalog.hive.HiveConstants.HIVE_FILTER_FIELD_PARAMS;
import static org.apache.gravitino.catalog.hive.HiveConstants.HIVE_METASTORE_URIS;
import static org.apache.gravitino.catalog.hive.HiveConstants.TABLE_TYPE;
import static org.apache.gravitino.catalog.hive.TableType.EXTERNAL_TABLE;
import static org.apache.gravitino.connector.BaseCatalog.CATALOG_BYPASS_PREFIX;
import static org.apache.gravitino.hive.HiveTable.SUPPORT_TABLE_TYPES;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.stream.Collectors;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.connector.CatalogInfo;
import org.apache.gravitino.connector.CatalogOperations;
import org.apache.gravitino.connector.HasPropertyMetadata;
import org.apache.gravitino.connector.SupportsSchemas;
import org.apache.gravitino.exceptions.ConnectionFailedException;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.NonEmptySchemaException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;
import org.apache.gravitino.hive.CachedClientPool;
import org.apache.gravitino.hive.HiveSchema;
import org.apache.gravitino.hive.HiveTable;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.types.Type;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Operations for interacting with an Apache Hive catalog in Apache Gravitino. */
public class HiveCatalogOperations implements CatalogOperations, SupportsSchemas, TableCatalog {

  public static final Logger LOG = LoggerFactory.getLogger(HiveCatalogOperations.class);
  public static final String GRAVITINO_KEYTAB_FORMAT = "keytabs/gravitino-hive-%s-keytab";

  @VisibleForTesting CachedClientPool clientPool;

  private CatalogInfo info;

  private HasPropertyMetadata propertiesMetadata;

  private String catalogName;

  private ScheduledThreadPoolExecutor checkTgtExecutor;
  private boolean listAllTables = true;
  // The maximum number of tables that can be returned by the listTableNamesByFilter function.
  // The default value is -1, which means that all tables are returned.
  private static final short MAX_TABLES = -1;

  // Map that maintains the mapping of keys in Gravitino to that in Hive, for example, users
  // will only need to set the configuration 'METASTORE_URL' in Gravitino and Gravitino will change
  // it to `METASTOREURIS` automatically and pass it to Hive.
  public static final Map<String, String> GRAVITINO_CONFIG_TO_HIVE =
      ImmutableMap.of(METASTORE_URIS, HIVE_METASTORE_URIS);

  /**
   * Initializes the Hive catalog operations with the provided configuration.
   *
   * @param conf The configuration map for the Hive catalog operations.
   * @param info The catalog info associated with this operations instance.
   * @param propertiesMetadata The properties metadata of Hive catalog.
   * @throws RuntimeException if initialization fails.
   */
  @Override
  public void initialize(
      Map<String, String> conf, CatalogInfo info, HasPropertyMetadata propertiesMetadata)
      throws RuntimeException {
    this.info = info;
    this.propertiesMetadata = propertiesMetadata;

    Properties prop = mergeProperties(conf);
    this.clientPool = new CachedClientPool(prop, conf);
    this.listAllTables = enableListAllTables(conf);

    // Initialize the HMS catalog name from catalog properties (default to DEFAULT_HMS_CATALOG)
    String defaultCatalog =
        (String)
            propertiesMetadata
                .catalogPropertiesMetadata()
                .getOrDefault(conf, HiveCatalogPropertiesMetadata.DEFAULT_CATALOG);
    this.catalogName = defaultCatalog;
  }

  @VisibleForTesting
  protected Properties mergeProperties(Map<String, String> conf) {
    // Key format like gravitino.bypass.a.b
    Map<String, String> byPassConfig = Maps.newHashMap();
    // Hold keys that lie in GRAVITINO_CONFIG_TO_HIVE
    Map<String, String> gravitinoConfig = Maps.newHashMap();

    conf.forEach(
        (key, value) -> {
          if (key.startsWith(CATALOG_BYPASS_PREFIX)) {
            // Trim bypass prefix and pass it to hive conf
            String hiveKey = key.substring(CATALOG_BYPASS_PREFIX.length());
            if (!hiveKey.isEmpty()) {
              byPassConfig.put(hiveKey, value);
            } else {
              LOG.warn("Ignoring invalid configuration key: {}", key);
            }
          } else if (GRAVITINO_CONFIG_TO_HIVE.containsKey(key)) {
            gravitinoConfig.put(GRAVITINO_CONFIG_TO_HIVE.get(key), value);
          }
        });

    Properties prop = new Properties();
    byPassConfig.forEach(prop::setProperty);
    gravitinoConfig.forEach(prop::setProperty);
    return prop;
  }

  boolean enableListAllTables(Map<String, String> conf) {
    return (boolean)
        propertiesMetadata.catalogPropertiesMetadata().getOrDefault(conf, LIST_ALL_TABLES);
  }
  /** Closes the Hive catalog and releases the associated client pool. */
  @Override
  public void close() {
    if (clientPool != null) {
      clientPool.close();
      clientPool = null;
    }

    if (checkTgtExecutor != null) {
      checkTgtExecutor.shutdown();
      checkTgtExecutor = null;
    }

    Path keytabPath = Paths.get(String.format(GRAVITINO_KEYTAB_FORMAT, info.id()));
    if (Files.exists(keytabPath)) {
      try {
        Files.delete(keytabPath);
      } catch (IOException e) {
        LOG.error("Fail to delete key tab file {}", keytabPath.toAbsolutePath(), e);
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
      NameIdentifier[] schemas =
          clientPool.run(
              c ->
                  c.getAllDatabases(catalogName).stream()
                      .map(db -> NameIdentifier.of(namespace, db))
                      .toArray(NameIdentifier[]::new));
      return schemas;
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Creates a new schema with the provided identifier, comment, and metadata.
   *
   * @param ident The identifier of the schema to create.
   * @param comment The comment for the schema.
   * @param properties The metadata properties for the schema.
   * @return The created {@link HiveSchema}.
   * @throws NoSuchCatalogException If the provided namespace is invalid or does not exist.
   * @throws SchemaAlreadyExistsException If a schema with the same name already exists.
   */
  @Override
  public HiveSchema createSchema(
      NameIdentifier ident, String comment, Map<String, String> properties)
      throws NoSuchCatalogException, SchemaAlreadyExistsException {
    try {
      HiveSchema hiveSchema =
          HiveSchema.builder()
              .withName(ident.name())
              .withComment(comment)
              .withCatalogName(catalogName)
              .withProperties(properties)
              .withAuditInfo(
                  AuditInfo.builder()
                      .withCreator(UserGroupInformation.getCurrentUser().getUserName())
                      .withCreateTime(Instant.now())
                      .build())
              .build();

      clientPool.run(
          client -> {
            client.createDatabase(hiveSchema);
            return null;
          });

      LOG.info("Created Hive schema (database) {} in Hive Metastore", ident.name());
      return hiveSchema;

    } catch (NoSuchCatalogException | SchemaAlreadyExistsException ne) {
      throw ne;
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
    try {
      HiveSchema database = clientPool.run(client -> client.getDatabase(catalogName, ident.name()));

      LOG.info("Loaded Hive schema (database) {} from Hive Metastore ", ident.name());
      return database;
    } catch (NoSuchEntityException e) {
      throw new NoSuchSchemaException("Schema %s does not exist", ident.name());
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
    try {
      // load the database parameters
      HiveSchema database = clientPool.run(client -> client.getDatabase(catalogName, ident.name()));
      Map<String, String> properties = database.properties();
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "Loaded properties for Hive schema (database) {} found {}",
            ident.name(),
            properties.keySet());
      }

      for (SchemaChange change : changes) {
        if (change instanceof SchemaChange.SetProperty) {
          properties.put(
              ((SchemaChange.SetProperty) change).getProperty(),
              ((SchemaChange.SetProperty) change).getValue());
        } else if (change instanceof SchemaChange.RemoveProperty) {
          properties.remove(((SchemaChange.RemoveProperty) change).getProperty());
        } else {
          throw new IllegalArgumentException(
              "Unsupported schema change type: " + change.getClass().getSimpleName());
        }
      }

      clientPool.run(
          client -> {
            client.alterDatabase(catalogName, ident.name(), database);
            return null;
          });

      LOG.info("Altered Hive schema (database) {} in Hive Metastore", ident.name());
      return database;

    } catch (InterruptedException e) {
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
    try {
      clientPool.run(
          client -> {
            client.dropDatabase(catalogName, ident.name(), cascade);
            return null;
          });
      LOG.info("Dropped Hive schema (database) {}", ident.name());
      return true;

    } catch (NoSuchSchemaException e) {
      return false;
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
      throw new NoSuchSchemaException("Schema (database) does not exist %s", namespace);
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
      List<String> allTables = clientPool.run(c -> c.getAllTables(catalogName, schemaIdent.name()));
      if (!listAllTables) {
        // The reason for using the listTableNamesByFilter function is that the
        // getTableObjectiesByName function has poor performance. Currently, we focus on the
        // Iceberg, Paimon and Hudi table. In the future, if necessary, we will need to filter out
        // other tables. In addition, the current return also includes tables of type VIRTUAL-VIEW.
        String icebergAndPaimonFilter = getIcebergAndPaimonFilter();
        List<String> icebergAndPaimonTables =
            clientPool.run(
                c ->
                    c.listTableNamesByFilter(
                        catalogName, schemaIdent.name(), icebergAndPaimonFilter, MAX_TABLES));
        allTables.removeAll(icebergAndPaimonTables);

        // filter out the Hudi tables
        String hudiFilter = String.format("%sprovider like \"hudi\"", HIVE_FILTER_FIELD_PARAMS);
        List<String> hudiTables =
            clientPool.run(
                c ->
                    c.listTableNamesByFilter(
                        catalogName, schemaIdent.name(), hudiFilter, MAX_TABLES));
        removeHudiTables(allTables, hudiTables);
      }
      return allTables.stream()
          .map(tbName -> NameIdentifier.of(namespace, tbName))
          .toArray(NameIdentifier[]::new);

    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private static String getIcebergAndPaimonFilter() {
    String icebergFilter = String.format("%stable_type like \"ICEBERG\"", HIVE_FILTER_FIELD_PARAMS);
    String paimonFilter = String.format("%stable_type like \"PAIMON\"", HIVE_FILTER_FIELD_PARAMS);
    return String.format("%s or %s", icebergFilter, paimonFilter);
  }

  private void removeHudiTables(List<String> allTables, List<String> hudiTables) {
    for (String hudiTable : hudiTables) {
      allTables.removeIf(
          t ->
              t.equals(hudiTable)
                  || t.startsWith(hudiTable + "_ro")
                  || t.startsWith(hudiTable + "_rt"));
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
    HiveTableHandle hiveTable = loadHiveTable(tableIdent);

    LOG.info("Loaded Hive table {} from Hive Metastore ", tableIdent.name());
    return hiveTable;
  }

  private HiveTableHandle loadHiveTable(NameIdentifier tableIdent) {
    NameIdentifier schemaIdent = NameIdentifier.of(tableIdent.namespace().levels());

    try {
      HiveTable table =
          clientPool.run(c -> c.getTable(catalogName, schemaIdent.name(), tableIdent.name()));
      return new HiveTableHandle(table, clientPool);

    } catch (InterruptedException e) {
      throw new RuntimeException(
          "Failed to load Hive table " + tableIdent.name() + " from Hive metastore", e);
    }
  }

  private void validatePartitionForCreate(Column[] columns, Transform[] partitioning) {
    int partitionStartIndex = columns.length - partitioning.length;

    for (int i = 0; i < partitioning.length; i++) {
      Preconditions.checkArgument(
          partitioning[i] instanceof Transforms.IdentityTransform,
          "Hive partition only supports identity transform");

      Transforms.IdentityTransform identity = (Transforms.IdentityTransform) partitioning[i];
      Preconditions.checkArgument(
          identity.fieldName().length == 1, "Hive partition does not support nested field");

      // The partition field must be placed at the end of the columns in order.
      // For example, if the table has columns [a, b, c, d], then the partition field must be
      // [b, c, d] or [c, d] or [d].
      Preconditions.checkArgument(
          columns[partitionStartIndex + i].name().equals(identity.fieldName()[0]),
          "The partition field must be placed at the end of the columns in order");
    }
  }

  private void validateColumnChangeForAlter(TableChange[] changes, HiveTable hiveTable) {
    Set<String> partitionFields = new HashSet<>(hiveTable.partitionFieldNames());
    Set<String> existingFields =
        Arrays.stream(hiveTable.columns()).map(Column::name).collect(Collectors.toSet());
    existingFields.addAll(partitionFields);

    Arrays.stream(changes)
        .filter(c -> c instanceof TableChange.ColumnChange)
        .forEach(
            c -> {
              String fieldName = String.join(".", ((TableChange.ColumnChange) c).fieldName());
              Preconditions.checkArgument(
                  c instanceof TableChange.UpdateColumnComment
                      || !partitionFields.contains(fieldName),
                  "Cannot alter partition column: " + fieldName);

              if (c instanceof TableChange.UpdateColumnPosition
                  && afterPartitionColumn(
                      partitionFields, ((TableChange.UpdateColumnPosition) c).getPosition())) {
                throw new IllegalArgumentException(
                    "Cannot alter column position to after partition column");
              }

              if (c instanceof TableChange.DeleteColumn) {
                existingFields.remove(fieldName);
              }

              if (c instanceof TableChange.AddColumn) {
                TableChange.AddColumn addColumn = (TableChange.AddColumn) c;

                if (existingFields.contains(fieldName)) {
                  throw new IllegalArgumentException(
                      "Cannot add column with duplicate name: " + fieldName);
                }

                if (addColumn.getPosition() == null) {
                  // If the position is not specified, the column will be added to the end of the
                  // non-partition columns.
                  return;
                }

                if ((afterPartitionColumn(partitionFields, addColumn.getPosition()))) {
                  throw new IllegalArgumentException("Cannot add column after partition column");
                }
              }
            });
  }

  private boolean afterPartitionColumn(
      Set<String> partitionFields, TableChange.ColumnPosition columnPosition) {
    Preconditions.checkArgument(columnPosition != null, "Column position cannot be null");

    if (columnPosition instanceof TableChange.After) {
      return partitionFields.contains(((TableChange.After) columnPosition).getColumn());
    }
    return false;
  }

  private void validateDistributionAndSort(Distribution distribution, SortOrder[] sortOrder) {
    if (distribution != Distributions.NONE) {
      boolean allNameReference =
          Arrays.stream(distribution.expressions())
              .allMatch(t -> t instanceof NamedReference.FieldReference);
      Preconditions.checkArgument(
          allNameReference, "Hive distribution only supports field reference");
    }

    if (ArrayUtils.isNotEmpty(sortOrder)) {
      boolean allNameReference =
          Arrays.stream(sortOrder)
              .allMatch(t -> t.expression() instanceof NamedReference.FieldReference);
      Preconditions.checkArgument(allNameReference, "Hive sort order only supports name reference");
    }
  }

  /**
   * Creates a new table in the Hive Metastore.
   *
   * @param tableIdent The identifier of the table to create.
   * @param columns The array of columns for the new table.
   * @param comment The comment for the new table.
   * @param properties The properties for the new table.
   * @param partitioning The partitioning for the new table.
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
      Transform[] partitioning,
      Distribution distribution,
      SortOrder[] sortOrders,
      Index[] indexes)
      throws NoSuchSchemaException, TableAlreadyExistsException {
    Preconditions.checkArgument(
        indexes.length == 0,
        "Hive-catalog does not support indexes, since indexing was removed since 3.0");
    NameIdentifier schemaIdent = NameIdentifier.of(tableIdent.namespace().levels());

    validatePartitionForCreate(columns, partitioning);
    validateDistributionAndSort(distribution, sortOrders);

    TableType tableType =
        (TableType)
            propertiesMetadata.tablePropertiesMetadata().getOrDefault(properties, TABLE_TYPE);
    Preconditions.checkArgument(
        SUPPORT_TABLE_TYPES.contains(tableType.name()),
        "Unsupported table type: " + tableType.name());

    try {
      if (!schemaExists(schemaIdent)) {
        LOG.warn("Hive schema (database) does not exist: {}", schemaIdent);
        throw new NoSuchSchemaException("Hive Schema (database) does not exist: %s ", schemaIdent);
      }

      HiveTable hiveTable =
          HiveTable.builder()
              .withName(tableIdent.name())
              .withCatalogName(catalogName)
              .withDatabaseName(schemaIdent.name())
              .withComment(comment)
              .withColumns(columns)
              .withProperties(properties)
              .withDistribution(distribution)
              .withSortOrders(sortOrders)
              .withAuditInfo(
                  AuditInfo.builder()
                      .withCreator(UserGroupInformation.getCurrentUser().getUserName())
                      .withCreateTime(Instant.now())
                      .build())
              .withPartitioning(partitioning)
              .build();
      clientPool.run(
          c -> {
            c.createTable(hiveTable);
            return null;
          });

      LOG.info("Created Hive table {} in Hive Metastore", tableIdent.name());
      return new HiveTableHandle(hiveTable, clientPool);

    } catch (IOException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Apply the {@link TableChange change} to an existing Hive table.
   *
   * <p>Note: When changing column position, since HMS will check the compatibility of column type
   * between the old column position and the new column position, you need to make sure that the new
   * column position is compatible with the old column position, otherwise the operation will fail
   * in HMS.
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
    NameIdentifier schemaIdent = NameIdentifier.of(tableIdent.namespace().levels());

    try {
      // TODO(@Minghuang): require a table lock to avoid race condition
      HiveTableHandle tableHandle = (HiveTableHandle) loadTable(tableIdent);
      HiveTable currentTable = tableHandle.table();
      validateColumnChangeForAlter(changes, currentTable);

      String newTableName = currentTable.name();
      String newComment = currentTable.comment();
      Map<String, String> updatedProperties = new HashMap<>(currentTable.properties());
      List<Column> updatedColumns = new ArrayList<>(Arrays.asList(currentTable.columns()));

      for (TableChange change : changes) {
        if (change instanceof TableChange.RenameTable) {
          TableChange.RenameTable rename = (TableChange.RenameTable) change;
          Preconditions.checkArgument(
              rename.getNewSchemaName().isEmpty(), "Does not support rename schema yet");
          newTableName = rename.getNewName();
        } else if (change instanceof TableChange.UpdateComment) {
          newComment = ((TableChange.UpdateComment) change).getNewComment();
        } else if (change instanceof TableChange.SetProperty) {
          TableChange.SetProperty setProperty = (TableChange.SetProperty) change;
          updatedProperties.put(setProperty.getProperty(), setProperty.getValue());
        } else if (change instanceof TableChange.RemoveProperty) {
          TableChange.RemoveProperty removeProperty = (TableChange.RemoveProperty) change;
          updatedProperties.remove(removeProperty.getProperty());
        } else if (change instanceof TableChange.ColumnChange) {
          applyColumnChange(updatedColumns, (TableChange.ColumnChange) change);
        } else {
          throw new IllegalArgumentException(
              "Unsupported table change type: "
                  + (change == null ? "null" : change.getClass().getSimpleName()));
        }
      }

      HiveTable updatedTable =
          buildAlteredHiveTable(
              currentTable, newTableName, newComment, updatedProperties, updatedColumns);

      HiveTable finalUpdatedTable = updatedTable;
      clientPool.run(
          c -> {
            c.alterTable(catalogName, schemaIdent.name(), tableIdent.name(), finalUpdatedTable);
            return null;
          });

      LOG.info("Altered Hive table {} in Hive Metastore", tableIdent.name());
      return new HiveTableHandle(updatedTable, clientPool);

    } catch (IllegalArgumentException e) {
      if (e.getMessage().contains("types incompatible with the existing columns")) {
        throw new IllegalArgumentException(
            "Failed to alter Hive table ["
                + tableIdent.name()
                + "] in Hive metastore, "
                + "since Hive metastore will check the compatibility of column type between the old and new column positions, "
                + "please ensure that the type of the new column position is compatible with the old one, "
                + "otherwise the alter operation will fail in Hive metastore.",
            e);
      }
      throw e;
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private HiveTable buildAlteredHiveTable(
      HiveTable original,
      String tableName,
      String comment,
      Map<String, String> properties,
      List<Column> columns) {
    HiveTable.Builder builder =
        HiveTable.builder()
            .withName(tableName)
            .withColumns(columns.toArray(new Column[0]))
            .withProperties(properties)
            .withAuditInfo(original.auditInfo())
            .withDistribution(original.distribution())
            .withSortOrders(original.sortOrder())
            .withPartitioning(original.partitioning())
            .withCatalogName(original.catalogName())
            .withDatabaseName(original.databaseName());

    if (comment != null) {
      builder.withComment(comment);
    }
    return builder.build();
  }

  private void applyColumnChange(List<Column> columns, TableChange.ColumnChange change) {
    if (change instanceof TableChange.AddColumn) {
      doAddColumn(columns, (TableChange.AddColumn) change);
    } else if (change instanceof TableChange.DeleteColumn) {
      doDeleteColumn(columns, (TableChange.DeleteColumn) change);
    } else if (change instanceof TableChange.RenameColumn) {
      doRenameColumn(columns, (TableChange.RenameColumn) change);
    } else if (change instanceof TableChange.UpdateColumnComment) {
      doUpdateColumnComment(columns, (TableChange.UpdateColumnComment) change);
    } else if (change instanceof TableChange.UpdateColumnPosition) {
      doUpdateColumnPosition(columns, (TableChange.UpdateColumnPosition) change);
    } else if (change instanceof TableChange.UpdateColumnType) {
      doUpdateColumnType(columns, (TableChange.UpdateColumnType) change);
    } else if (change instanceof TableChange.UpdateColumnDefaultValue) {
      doUpdateColumnDefaultValue(columns, (TableChange.UpdateColumnDefaultValue) change);
    } else if (change instanceof TableChange.UpdateColumnNullability) {
      doUpdateColumnNullability(columns, (TableChange.UpdateColumnNullability) change);
    } else if (change instanceof TableChange.UpdateColumnAutoIncrement) {
      throw new IllegalArgumentException("Hive does not support altering column auto increment");
    } else {
      throw new IllegalArgumentException(
          "Unsupported column change type: " + change.getClass().getSimpleName());
    }
  }

  private int columnPosition(List<Column> columns, TableChange.ColumnPosition position) {
    if (position == null || position instanceof TableChange.Default) {
      return columns.size();
    } else if (position instanceof TableChange.After) {
      String afterColumn = ((TableChange.After) position).getColumn();
      int indexOfColumn = indexOfColumn(columns, afterColumn);
      Preconditions.checkArgument(indexOfColumn != -1, "Column does not exist: " + afterColumn);
      return indexOfColumn + 1;
    } else if (position instanceof TableChange.First) {
      return 0;
    } else {
      throw new UnsupportedOperationException(
          "Unsupported column position type: " + position.getClass().getSimpleName());
    }
  }

  private int indexOfColumn(List<Column> columns, String fieldName) {
    for (int i = 0; i < columns.size(); i++) {
      if (columns.get(i).name().equals(fieldName)) {
        return i;
      }
    }
    return -1;
  }

  private void doAddColumn(List<Column> columns, TableChange.AddColumn change) {
    String columnName = topLevelFieldName(change.getFieldName());
    if (change.isAutoIncrement()) {
      throw new IllegalArgumentException("Hive catalog does not support auto-increment column");
    }
    Preconditions.checkArgument(
        indexOfColumn(columns, columnName) == -1, "Column already exists: " + columnName);
    int targetPosition = columnPosition(columns, change.getPosition());
    columns.add(
        targetPosition,
        Column.of(
            columnName,
            change.getDataType(),
            change.getComment(),
            change.isNullable(),
            change.isAutoIncrement(),
            defaultValueOrUnset(change.getDefaultValue())));
  }

  private void doDeleteColumn(List<Column> columns, TableChange.DeleteColumn change) {
    String columnName = topLevelFieldName(change.fieldName());
    int index = indexOfColumn(columns, columnName);
    if (index == -1) {
      if (!change.getIfExists()) {
        throw new IllegalArgumentException("DeleteColumn does not exist: " + columnName);
      }
      return;
    }
    columns.remove(index);
  }

  private void doRenameColumn(List<Column> columns, TableChange.RenameColumn change) {
    String columnName = topLevelFieldName(change.fieldName());
    int index = indexOfColumn(columns, columnName);
    if (index == -1) {
      throw new IllegalArgumentException("RenameColumn does not exist: " + columnName);
    }
    String newName = change.getNewName();
    Preconditions.checkArgument(
        indexOfColumn(columns, newName) == -1, "Column already exists: " + newName);
    Column existing = columns.get(index);
    columns.set(index, rebuildColumn(existing, newName, null, null, null, null, null));
  }

  private void doUpdateColumnComment(List<Column> columns, TableChange.UpdateColumnComment change) {
    int index = indexOfColumn(columns, topLevelFieldName(change.fieldName()));
    if (index == -1) {
      throw new IllegalArgumentException(
          "UpdateColumnComment does not exist: " + change.fieldName()[0]);
    }
    Column existing = columns.get(index);
    columns.set(
        index, rebuildColumn(existing, null, null, change.getNewComment(), null, null, null));
  }

  private void doUpdateColumnPosition(
      List<Column> columns, TableChange.UpdateColumnPosition change) {
    String columnName = topLevelFieldName(change.fieldName());
    int sourceIndex = indexOfColumn(columns, columnName);
    if (sourceIndex == -1) {
      throw new IllegalArgumentException("UpdateColumnPosition does not exist: " + columnName);
    }
    Column column = columns.remove(sourceIndex);
    int targetIndex = columnPosition(columns, change.getPosition());
    columns.add(targetIndex, column);
  }

  private void doUpdateColumnType(List<Column> columns, TableChange.UpdateColumnType change) {
    String columnName = topLevelFieldName(change.getFieldName());
    int index = indexOfColumn(columns, columnName);
    if (index == -1) {
      throw new IllegalArgumentException("UpdateColumnType does not exist: " + columnName);
    }
    Column existing = columns.get(index);
    columns.set(
        index, rebuildColumn(existing, null, change.getNewDataType(), null, null, null, null));
  }

  private void doUpdateColumnDefaultValue(
      List<Column> columns, TableChange.UpdateColumnDefaultValue change) {
    String columnName = topLevelFieldName(change.fieldName());
    int index = indexOfColumn(columns, columnName);
    if (index == -1) {
      throw new IllegalArgumentException("UpdateColumnDefaultValue does not exist: " + columnName);
    }
    Column existing = columns.get(index);
    columns.set(
        index, rebuildColumn(existing, null, null, null, null, null, change.getNewDefaultValue()));
  }

  private void doUpdateColumnNullability(
      List<Column> columns, TableChange.UpdateColumnNullability change) {
    String columnName = topLevelFieldName(change.fieldName());
    int index = indexOfColumn(columns, columnName);
    if (index == -1) {
      throw new IllegalArgumentException("UpdateColumnNullability does not exist: " + columnName);
    }
    Column existing = columns.get(index);
    columns.set(index, rebuildColumn(existing, null, null, null, change.nullable(), null, null));
  }

  private Column rebuildColumn(
      Column existing,
      String name,
      Type dataType,
      String comment,
      Boolean nullable,
      Boolean autoIncrement,
      Expression defaultValue) {
    return Column.of(
        name != null ? name : existing.name(),
        dataType != null ? dataType : existing.dataType(),
        comment != null ? comment : existing.comment(),
        nullable != null ? nullable : existing.nullable(),
        autoIncrement != null ? autoIncrement : existing.autoIncrement(),
        defaultValue != null ? defaultValue : existing.defaultValue());
  }

  private Expression defaultValueOrUnset(Expression defaultValue) {
    return defaultValue == null ? Column.DEFAULT_VALUE_NOT_SET : defaultValue;
  }

  private String topLevelFieldName(String[] fieldName) {
    Preconditions.checkArgument(
        fieldName.length == 1, "Hive catalog only supports top-level column operations");
    return fieldName[0];
  }

  /**
   * Drops a table from the Hive Metastore. Deletes the table and removes the directory associated
   * with the table from the file system if the table is not EXTERNAL table. In case of an external
   * table, only the associated metadata information is removed.
   *
   * @param tableIdent The identifier of the table to drop.
   * @return true if the table is successfully dropped; false if the table does not exist.
   */
  @Override
  public boolean dropTable(NameIdentifier tableIdent) {
    try {
      if (isExternalTable(tableIdent)) {
        return dropHiveTable(tableIdent, false, false);
      } else {
        return dropHiveTable(tableIdent, true, false);
      }
    } catch (NoSuchTableException e) {
      return false;
    }
  }

  /**
   * Purges a table from the Hive Metastore. Completely purge the table skipping trash for managed
   * table, external table aren't supported to purge table.
   *
   * @param tableIdent The identifier of the table to purge.
   * @return true if the table is successfully purged; false if the table does not exist.
   * @throws UnsupportedOperationException If the table type is EXTERNAL_TABLE, it cannot be purged.
   */
  @Override
  public boolean purgeTable(NameIdentifier tableIdent) throws UnsupportedOperationException {
    try {
      if (isExternalTable(tableIdent)) {
        throw new UnsupportedOperationException("Can't purge a external hive table");
      } else {
        return dropHiveTable(tableIdent, true, true);
      }
    } catch (NoSuchTableException e) {
      return false;
    }
  }

  /**
   * Performs `getAllDatabases` operation in Hive Metastore to test the connection.
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
      clientPool.run(c -> c.getAllDatabases(catalogName));
    } catch (ConnectionFailedException e) {
      throw e;
    } catch (Exception e) {
      throw new ConnectionFailedException(
          e, "Failed to run getAllDatabases in Hive Metastore: %s", e.getMessage());
    }
  }

  /**
   * Checks if the given namespace is a valid namespace for the Hive schema.
   *
   * @param tableIdent The namespace to validate.
   * @param deleteData Whether to delete the table data.
   * @param ifPurge Whether to purge the table.
   * @return true if the namespace is valid; otherwise, false.
   */
  private boolean dropHiveTable(NameIdentifier tableIdent, boolean deleteData, boolean ifPurge) {
    NameIdentifier schemaIdent = NameIdentifier.of(tableIdent.namespace().levels());

    try {
      clientPool.run(
          c -> {
            c.dropTable(catalogName, schemaIdent.name(), tableIdent.name(), deleteData, ifPurge);
            return null;
          });

      LOG.info("Dropped Hive table {}", tableIdent.name());
      return true;

    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  CachedClientPool getClientPool() {
    return clientPool;
  }

  private boolean isExternalTable(NameIdentifier tableIdent) {
    HiveTableHandle hiveTable = loadHiveTable(tableIdent);
    return EXTERNAL_TABLE.name().equalsIgnoreCase(hiveTable.getTableType());
  }

  private static ThreadFactory getThreadFactory(String factoryName) {
    return new ThreadFactoryBuilder().setDaemon(true).setNameFormat(factoryName + "-%d").build();
  }
}
