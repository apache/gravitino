/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.gravitino.catalog.lakehouse.lance;

import static org.apache.gravitino.lance.common.utils.LanceConstants.LANCE_CREATION_MODE;
import static org.apache.gravitino.lance.common.utils.LanceConstants.LANCE_TABLE_REGISTER;
import static org.apache.gravitino.rel.Column.DEFAULT_VALUE_NOT_SET;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.ManagedSchemaOperations;
import org.apache.gravitino.catalog.ManagedTableOperations;
import org.apache.gravitino.connector.GenericColumn;
import org.apache.gravitino.connector.GenericTable;
import org.apache.gravitino.connector.SupportsSchemas;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;
import org.apache.gravitino.lance.common.ops.gravitino.LanceDataTypeConverter;
import org.apache.gravitino.lance.common.utils.LanceConstants;
import org.apache.gravitino.lance.common.utils.LancePropertiesUtils;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.ColumnEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.utils.PrincipalUtils;
import org.lance.Dataset;
import org.lance.ReadOptions;
import org.lance.WriteParams;
import org.lance.index.DistanceType;
import org.lance.index.IndexOptions;
import org.lance.index.IndexParams;
import org.lance.index.IndexType;
import org.lance.index.vector.VectorIndexParams;
import org.lance.schema.ColumnAlteration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LanceTableOperations extends ManagedTableOperations {
  private static final Logger LOG = LoggerFactory.getLogger(LanceTableOperations.class);

  public enum CreationMode {
    CREATE,
    EXIST_OK,
    OVERWRITE
  }

  /**
   * Controls when Gravitino refreshes a Lance table's stored columns from the underlying dataset.
   *
   * <p>{@link #DECLARED_AND_EMPTY} is the default. It covers two complementary repair cases:
   *
   * <ul>
   *   <li><b>Declared tables</b> ({@code lance.declared=true}): metadata-only tables whose schema
   *       has not yet been written to Gravitino (e.g. Spark staged-create flow).
   *   <li><b>Empty stored columns</b>: tables where Gravitino has no column metadata but the Lance
   *       dataset at the table's location already carries a real schema (e.g. tables registered via
   *       the register-table path before schema was captured).
   * </ul>
   *
   * <p><b>Zero-column Lance dataset:</b> when the dataset has no columns, Gravitino records the
   * checked dataset version ({@code lance.version}) without modifying stored columns. Subsequent
   * {@code loadTable} calls skip the dataset open because the stored version acts as a "confirmed
   * empty" marker. The dataset is re-examined only after the stored version changes (for example
   * via an explicit {@code alterTable}) or when the mode is switched to {@link #VERSION_CHECK}.
   */
  public enum SchemaRefreshMode {
    /**
     * Default mode. Refreshes stored columns from the Lance dataset for declared tables ({@code
     * lance.declared=true}) and for tables whose Gravitino column list is empty. Also repairs
     * empty-column tables that were registered before their schema was captured.
     */
    DECLARED_AND_EMPTY,
    /**
     * Opens the Lance dataset on every {@code loadTable}, compares the dataset version with the
     * stored {@code lance.version}, and refreshes columns when the version has changed. The version
     * is the sole gating factor: if the version is unchanged the schema read is skipped even when
     * stored columns are empty.
     */
    VERSION_CHECK
  }

  private final EntityStore store;

  private final ManagedSchemaOperations schemaOps;

  private final IdGenerator idGenerator;

  private volatile Map<String, String> catalogProperties = Map.of();

  public LanceTableOperations(
      EntityStore store, ManagedSchemaOperations schemaOps, IdGenerator idGenerator) {
    this.store = store;
    this.schemaOps = schemaOps;
    this.idGenerator = idGenerator;
  }

  @Override
  protected EntityStore store() {
    return store;
  }

  @Override
  protected SupportsSchemas schemas() {
    return schemaOps;
  }

  @Override
  protected IdGenerator idGenerator() {
    return idGenerator;
  }

  /**
   * Sets the catalog properties used to resolve Lance storage defaults at runtime.
   *
   * @param catalogProperties the catalog properties
   */
  public void setCatalogProperties(Map<String, String> catalogProperties) {
    this.catalogProperties =
        catalogProperties == null ? Map.of() : ImmutableMap.copyOf(catalogProperties);
  }

  @Override
  public Table loadTable(NameIdentifier ident) throws NoSuchTableException {
    Table table = super.loadTable(ident);
    // Spark staged create can write the actual schema only to the Lance dataset path. Refresh
    // Gravitino metadata when the stored table is declared-only, empty, or configured to track
    // Lance dataset versions.
    boolean declaredOnly = isDeclaredOnly(table);
    boolean emptySchema = table.columns().length == 0;
    SchemaRefreshMode refreshMode = schemaRefreshMode();
    if (!declaredOnly && !emptySchema && refreshMode == SchemaRefreshMode.DECLARED_AND_EMPTY) {
      return table;
    }
    // Empty-schema table that was already confirmed against a stored version: skip the dataset
    // open. The stored lance.version acts as a "checked at this version" marker written on the
    // first confirmation. VERSION_CHECK mode does not take this shortcut — it opens the dataset
    // every time to compare the current version.
    if (!declaredOnly
        && emptySchema
        && StringUtils.isNotBlank(table.properties().get(LanceConstants.LANCE_TABLE_VERSION))
        && refreshMode == SchemaRefreshMode.DECLARED_AND_EMPTY) {
      return table;
    }

    String location = table.properties().get(Table.PROPERTY_LOCATION);
    if (StringUtils.isBlank(location)) {
      return table;
    }

    Map<String, String> storageOptions =
        LancePropertiesUtils.resolveLanceStorageOptions(catalogProperties, table.properties());
    Column[] columns;
    long datasetVersion;
    try (Dataset dataset = openDataset(location, storageOptions)) {
      datasetVersion = dataset.version();
      if (refreshMode == SchemaRefreshMode.VERSION_CHECK
          && !declaredOnly
          && !isDatasetVersionChanged(table, datasetVersion)) {
        return table;
      }
      columns = extractColumns(dataset.getSchema());
    } catch (Exception e) {
      LOG.debug(
          "Failed to load Lance schema from location {} for table {}. Return stored metadata.",
          location,
          ident,
          e);
      return table;
    }

    if (columns.length == 0) {
      // Dataset is genuinely empty: record the checked version so future DECLARED_AND_EMPTY loads
      // can skip the dataset open (see the early-return above). Declared tables are excluded
      // because their lance.declared flag is the authoritative "not yet written" signal.
      if (!declaredOnly) {
        return recordCheckedEmptyVersion(ident, datasetVersion);
      }
      return table;
    }

    return repairTableMetadata(ident, columns, datasetVersion);
  }

  @Override
  public Table createTable(
      NameIdentifier ident,
      Column[] columns,
      String comment,
      Map<String, String> properties,
      Transform[] partitions,
      Distribution distribution,
      SortOrder[] sortOrders,
      Index[] indexes)
      throws NoSuchSchemaException, TableAlreadyExistsException {
    String location = properties.get(Table.PROPERTY_LOCATION);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(location), "Table location must be specified");

    // Extract creation mode from properties
    CreationMode mode =
        Optional.ofNullable(properties.get(LANCE_CREATION_MODE))
            .map(CreationMode::valueOf)
            .orElse(CreationMode.CREATE);

    boolean register =
        Optional.ofNullable(properties.get(LANCE_TABLE_REGISTER))
            .map(Boolean::parseBoolean)
            .orElse(false);

    // Handle EXIST_OK mode - check if table exists first
    if (mode == CreationMode.EXIST_OK) {
      Preconditions.checkArgument(
          !register, "EXIST_OK mode is not supported for register operation");

      try {
        return super.loadTable(ident);
      } catch (NoSuchTableException e) {
        // Table doesn't exist, proceed with creation
      }
    }

    // Handle OVERWRITE mode - drop existing table if present
    if (mode == CreationMode.OVERWRITE) {
      if (register) {
        dropTable(ident);
      } else {
        purgeTable(ident);
      }
    }

    // Now create the table (for all modes after handling above)
    return createTableInternal(
        ident,
        columns,
        comment,
        properties,
        partitions,
        distribution,
        sortOrders,
        indexes,
        register,
        location);
  }

  @Override
  public Table alterTable(NameIdentifier ident, TableChange... changes)
      throws NoSuchSchemaException, TableAlreadyExistsException {

    Table loadedTable = super.loadTable(ident);
    long version = handleLanceTableChange(loadedTable, changes);
    // After making changes to the Lance dataset, we need to update the table metadata in
    // Gravitino. If there's any failure during this process, the code will throw an exception
    // and the update won't be applied in Gravitino.
    TableChange[] metadataChanges = Arrays.copyOf(changes, changes.length + 1);
    metadataChanges[changes.length] =
        TableChange.setProperty(LanceConstants.LANCE_TABLE_VERSION, String.valueOf(version));
    return super.alterTable(ident, metadataChanges);
  }

  @Override
  public boolean purgeTable(NameIdentifier ident) {
    try {
      // Use super.loadTable to avoid triggering an unnecessary schema-refresh (which may open the
      // dataset) for a table that is about to be deleted anyway.
      Table table = super.loadTable(ident);
      boolean external =
          Optional.ofNullable(table.properties().get(Table.PROPERTY_EXTERNAL))
              .map(Boolean::parseBoolean)
              .orElse(false);

      // NOTE: the two steps below (metadata removal and dataset deletion) are NOT atomic.
      // If the process crashes between them, the entity-store record will be gone but the
      // Lance dataset will still exist on storage (orphaned data), or vice-versa. There is
      // currently no recovery mechanism for this window.
      boolean purged = super.purgeTable(ident);
      // If the table is a managed table, super.purgeTable will call dropTable to remove the
      // underlying Lance dataset, so we don't need to do anything here.
      if (!external) {
        return purged;
      }

      // If the table metadata is purged successfully, we can delete the Lance dataset.
      // Otherwise, we should not delete the dataset.
      if (purged) {
        dropLanceDataset(table);
      }

      return purged;

    } catch (NoSuchTableException e) {
      return false;
    } catch (Exception e) {
      throw new RuntimeException("Failed to purge Lance dataset for table " + ident, e);
    }
  }

  @Override
  public boolean dropTable(NameIdentifier ident) {
    try {
      // Use super.loadTable to skip schema-refresh overhead when dropping.
      Table table = super.loadTable(ident);
      boolean external =
          Optional.ofNullable(table.properties().get(Table.PROPERTY_EXTERNAL))
              .map(Boolean::parseBoolean)
              .orElse(false);

      // NOTE: the two steps below (metadata removal and dataset deletion) are NOT atomic.
      // If the process crashes between them, the entity-store record will be gone but the
      // Lance dataset will still exist on storage (orphaned data), or vice-versa. There is
      // currently no recovery mechanism for this window.
      boolean dropped = super.dropTable(ident);
      if (external) {
        return dropped;
      }

      // If the table metadata is dropped successfully, and the table is not external, we can
      // delete the Lance dataset. Otherwise, we should not delete the dataset.
      if (dropped) {
        dropLanceDataset(table);
      }

      return dropped;

    } catch (NoSuchTableException e) {
      return false;
    } catch (Exception e) {
      throw new RuntimeException("Failed to drop Lance dataset for table " + ident, e);
    }
  }

  private void dropLanceDataset(Table table) {
    String location = table.properties().get(Table.PROPERTY_LOCATION);
    Map<String, String> resolvedStorageOptions =
        LancePropertiesUtils.resolveLanceStorageOptions(catalogProperties, table.properties());
    try {
      Dataset.drop(location, resolvedStorageOptions);
      LOG.info("Deleted Lance dataset at location {}", location);
    } catch (Exception e) {
      // Dataset.drop (native) throws IOException with "Not found:" when path doesn't exist.
      if (e instanceof IOException
          && e.getMessage() != null
          && e.getMessage().contains("Not found:")) {
        LOG.warn("Lance dataset at {} was already deleted, skipping.", location);
      } else {
        throw new RuntimeException("Failed to delete Lance dataset at " + location, e);
      }
    }
  }

  // Package-private for testing
  Table createTableInternal(
      NameIdentifier ident,
      Column[] columns,
      String comment,
      Map<String, String> properties,
      Transform[] partitions,
      Distribution distribution,
      SortOrder[] sortOrders,
      Index[] indexes,
      boolean register,
      String location)
      throws NoSuchSchemaException, TableAlreadyExistsException {

    if (register) {
      // Currently, register operation does not read the schema from the underlying Lance dataset.
      // So we can't get the version of the dataset here.
      return super.createTable(
          ident, columns, comment, properties, partitions, distribution, sortOrders, indexes);
    }

    // Check whether it's a metadata-only declare table operation.
    boolean declaredOnly =
        Optional.ofNullable(properties.get(LanceConstants.LANCE_TABLE_DECLARED))
            .map(Boolean::parseBoolean)
            .orElse(false);
    if (declaredOnly) {
      // For declare table, we just create the table metadata in Gravitino without creating the
      // underlying Lance dataset.
      return super.createTable(
          ident, columns, comment, properties, partitions, distribution, sortOrders, indexes);
    }

    Map<String, String> storageProps =
        LancePropertiesUtils.resolveLanceStorageOptions(catalogProperties, properties);
    try (Dataset ignored =
        Dataset.write()
            .schema(convertColumnsToArrowSchema(columns))
            .uri(location)
            .mode(WriteParams.WriteMode.CREATE)
            .storageOptions(storageProps)
            .execute()) {
      // Only create the table metadata in Gravitino after the Lance dataset is successfully
      // created.
      long datasetVersion = ignored.version();
      Map<String, String> updatedProperties =
          ImmutableMap.<String, String>builder()
              .putAll(properties)
              .put(LanceConstants.LANCE_TABLE_VERSION, String.valueOf(datasetVersion))
              .build();
      return super.createTable(
          ident,
          columns,
          comment,
          updatedProperties,
          partitions,
          distribution,
          sortOrders,
          indexes);
    } catch (NoSuchSchemaException e) {
      throw e;
    } catch (TableAlreadyExistsException e) {
      // If the table metadata already exists, but the underlying lance table was just created
      // successfully, we need to clean up the created lance table to avoid orphaned datasets.
      Dataset.drop(location, storageProps);
      throw e;
    } catch (IllegalArgumentException e) {
      if (e.getMessage().contains("Dataset already exists")) {
        throw new TableAlreadyExistsException(
            e, "Lance dataset already exists at location %s", location);
      }
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Failed to create Lance dataset at location " + location, e);
    }
  }

  private Schema convertColumnsToArrowSchema(Column[] columns) {
    List<Field> fields =
        Arrays.stream(columns)
            .map(
                col ->
                    LanceDataTypeConverter.CONVERTER.toArrowField(
                        col.name(), col.dataType(), col.nullable()))
            .collect(Collectors.toList());
    return new Schema(fields);
  }

  private SchemaRefreshMode schemaRefreshMode() {
    return Optional.ofNullable(catalogProperties.get(LanceConstants.LANCE_SCHEMA_REFRESH_MODE))
        .map(mode -> mode.trim().replace('-', '_').toUpperCase())
        .map(SchemaRefreshMode::valueOf)
        .orElse(SchemaRefreshMode.DECLARED_AND_EMPTY);
  }

  private boolean isDeclaredOnly(Table table) {
    return isDeclaredOnly(table.properties());
  }

  private boolean isDeclaredOnly(Map<String, String> properties) {
    return Optional.ofNullable(properties.get(LanceConstants.LANCE_TABLE_DECLARED))
        .map(Boolean::parseBoolean)
        .orElse(false);
  }

  private boolean isDatasetVersionChanged(Table table, long datasetVersion) {
    return isDatasetVersionChanged(table.properties(), datasetVersion);
  }

  private boolean isDatasetVersionChanged(Map<String, String> properties, long datasetVersion) {
    String version = properties.get(LanceConstants.LANCE_TABLE_VERSION);
    if (StringUtils.isBlank(version)) {
      return true;
    }

    try {
      return Long.parseLong(version) != datasetVersion;
    } catch (NumberFormatException e) {
      return true;
    }
  }

  private Column[] extractColumns(Schema arrowSchema) {
    return arrowSchema.getFields().stream()
        .map(
            field ->
                Column.of(
                    field.getName(),
                    LanceDataTypeConverter.CONVERTER.toGravitino(field),
                    null,
                    field.isNullable(),
                    false,
                    DEFAULT_VALUE_NOT_SET))
        .toArray(Column[]::new);
  }

  private Table repairTableMetadata(NameIdentifier ident, Column[] columns, long datasetVersion) {
    try {
      TableEntity tableEntity =
          store.update(
              ident,
              TableEntity.class,
              Entity.EntityType.TABLE,
              current -> {
                if (!needsSchemaRefresh(current, datasetVersion)) {
                  return current;
                }
                return replaceColumnsFromDataset(current, columns, datasetVersion);
              });
      return toGenericTable(tableEntity);
    } catch (NoSuchEntityException e) {
      throw new NoSuchTableException(e, "Table %s does not exist", ident);
    } catch (EntityAlreadyExistsException e) {
      throw new IllegalArgumentException("Failed to repair table " + ident, e);
    } catch (IOException e) {
      throw new RuntimeException("Failed to repair table " + ident, e);
    }
  }

  private Table recordCheckedEmptyVersion(NameIdentifier ident, long datasetVersion) {
    try {
      TableEntity tableEntity =
          store.update(
              ident,
              TableEntity.class,
              Entity.EntityType.TABLE,
              current -> {
                if (!isDatasetVersionChanged(current.properties(), datasetVersion)) {
                  return current;
                }
                Map<String, String> updatedProperties = new HashMap<>(current.properties());
                updatedProperties.put(
                    LanceConstants.LANCE_TABLE_VERSION, String.valueOf(datasetVersion));
                return TableEntity.builder()
                    .withId(current.id())
                    .withName(current.name())
                    .withNamespace(current.namespace())
                    .withComment(current.comment())
                    .withColumns(current.columns())
                    .withProperties(updatedProperties)
                    .withPartitioning(current.partitioning())
                    .withDistribution(current.distribution())
                    .withSortOrders(current.sortOrders())
                    .withIndexes(current.indexes())
                    .withAuditInfo(
                        AuditInfo.builder()
                            .withCreator(current.auditInfo().creator())
                            .withCreateTime(current.auditInfo().createTime())
                            .withLastModifier(PrincipalUtils.getCurrentPrincipal().getName())
                            .withLastModifiedTime(Instant.now())
                            .build())
                    .build();
              });
      return toGenericTable(tableEntity);
    } catch (NoSuchEntityException e) {
      throw new NoSuchTableException(e, "Table %s does not exist", ident);
    } catch (EntityAlreadyExistsException e) {
      throw new IllegalArgumentException("Failed to record empty version for table " + ident, e);
    } catch (IOException e) {
      throw new RuntimeException("Failed to record empty version for table " + ident, e);
    }
  }

  private boolean needsSchemaRefresh(TableEntity tableEntity, long datasetVersion) {
    return isDeclaredOnly(tableEntity.properties())
        || tableEntity.columns().isEmpty()
        || isDatasetVersionChanged(tableEntity.properties(), datasetVersion);
  }

  private TableEntity replaceColumnsFromDataset(
      TableEntity tableEntity, Column[] columns, long datasetVersion) {
    Map<String, String> updatedProperties = new HashMap<>(tableEntity.properties());
    updatedProperties.put(LanceConstants.LANCE_TABLE_VERSION, String.valueOf(datasetVersion));
    updatedProperties.remove(LanceConstants.LANCE_TABLE_DECLARED);

    AuditInfo columnAuditInfo =
        AuditInfo.builder()
            .withCreator(PrincipalUtils.getCurrentPrincipal().getName())
            .withCreateTime(Instant.now())
            .build();
    List<ColumnEntity> columnEntities =
        IntStream.range(0, columns.length)
            .mapToObj(
                i ->
                    ColumnEntity.toColumnEntity(
                        columns[i], i, idGenerator.nextId(), columnAuditInfo))
            .collect(Collectors.toList());

    return TableEntity.builder()
        .withId(tableEntity.id())
        .withName(tableEntity.name())
        .withNamespace(tableEntity.namespace())
        .withComment(tableEntity.comment())
        .withColumns(columnEntities)
        .withProperties(updatedProperties)
        .withPartitioning(tableEntity.partitioning())
        .withDistribution(tableEntity.distribution())
        .withSortOrders(tableEntity.sortOrders())
        .withIndexes(tableEntity.indexes())
        .withAuditInfo(
            AuditInfo.builder()
                .withCreator(tableEntity.auditInfo().creator())
                .withCreateTime(tableEntity.auditInfo().createTime())
                .withLastModifier(PrincipalUtils.getCurrentPrincipal().getName())
                .withLastModifiedTime(Instant.now())
                .build())
        .build();
  }

  private GenericTable toGenericTable(TableEntity tableEntity) {
    return GenericTable.builder()
        .withName(tableEntity.name())
        .withComment(tableEntity.comment())
        .withColumns(
            tableEntity.columns().stream().map(this::toGenericColumn).toArray(Column[]::new))
        .withProperties(tableEntity.properties())
        .withAuditInfo(tableEntity.auditInfo())
        .withSortOrders(tableEntity.sortOrders())
        .withPartitioning(tableEntity.partitioning())
        .withDistribution(tableEntity.distribution())
        .withIndexes(tableEntity.indexes())
        .build();
  }

  private GenericColumn toGenericColumn(ColumnEntity columnEntity) {
    return GenericColumn.builder()
        .withName(columnEntity.name())
        .withComment(columnEntity.comment())
        .withAutoIncrement(columnEntity.autoIncrement())
        .withNullable(columnEntity.nullable())
        .withType(columnEntity.dataType())
        .withDefaultValue(columnEntity.defaultValue())
        .build();
  }

  /**
   * Handle the table changes on the underlying Lance dataset.
   *
   * <p>Note: this method can't guarantee the atomicity of the operations on Lance dataset. For
   * example, only a subset of changes may be applied if an exception occurs during the process.
   *
   * @param table the table to be altered
   * @param changes the changes to be applied
   * @return the new version id of the Lance dataset after applying the changes
   */
  long handleLanceTableChange(Table table, TableChange[] changes) {
    String location = table.properties().get(Table.PROPERTY_LOCATION);
    Map<String, String> storageOptions =
        LancePropertiesUtils.resolveLanceStorageOptions(catalogProperties, table.properties());
    try (Dataset dataset = openDataset(location, storageOptions)) {
      for (TableChange change : changes) {
        if (change instanceof TableChange.DeleteColumn deleteColumn) {
          dataset.dropColumns(List.of(String.join(".", deleteColumn.fieldName())));
        } else if (change instanceof TableChange.AddIndex addIndex) {
          IndexType indexType = IndexType.valueOf(addIndex.getType().name());
          IndexParams indexParams = getIndexParamsByIndexType(indexType);
          dataset.createIndex(
              IndexOptions.builder(
                      Arrays.stream(addIndex.getFieldNames())
                          .map(field -> String.join(".", field))
                          .collect(Collectors.toList()),
                      indexType,
                      indexParams)
                  .replace(true)
                  .withIndexName(addIndex.getName())
                  .build());
        } else if (change instanceof TableChange.RenameColumn renameColumn) {
          ColumnAlteration lanceColumnAlter =
              new ColumnAlteration.Builder(String.join(".", renameColumn.fieldName()))
                  .rename(renameColumn.getNewName())
                  .build();
          dataset.alterColumns(List.of(lanceColumnAlter));
        } else {
          // Currently, only column drop/rename and index addition are supported.
          // TODO: Support change column type once we have a clear knowledge about the means of
          // castTo in Lance.
          throw new UnsupportedOperationException(
              "Unsupported changes to lance table: " + change.getClass().getSimpleName());
        }
      }
      return dataset.getVersion().getId();
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(
          "Failed to handle alterations to Lance dataset at location " + location, e);
    }
  }

  Dataset openDataset(String location) {
    return openDataset(location, Map.of());
  }

  Dataset openDataset(String location, Map<String, String> storageOptions) {
    // Do not pass an explicit allocator: OpenDatasetBuilder then sets selfManagedAllocator=true
    // so Dataset.close() will release the off-heap memory. Passing new RootAllocator() sets
    // selfManagedAllocator=false and leaks the allocator on every call.
    return Dataset.open()
        .uri(location)
        .readOptions(new ReadOptions.Builder().setStorageOptions(storageOptions).build())
        .build();
  }

  private IndexParams getIndexParamsByIndexType(IndexType indexType) {
    switch (indexType) {
      case SCALAR:
        return IndexParams.builder().build();
      case VECTOR:
        // TODO make these parameters configurable
        int numberOfDimensions = 3; // this value should be determined dynamically based on the data
        // Add properties to Index to set this value.
        return IndexParams.builder()
            .setVectorIndexParams(
                VectorIndexParams.ivfPq(2, 8, numberOfDimensions, DistanceType.L2, 2))
            .build();
      default:
        throw new IllegalArgumentException("Unsupported index type: " + indexType);
    }
  }
}
