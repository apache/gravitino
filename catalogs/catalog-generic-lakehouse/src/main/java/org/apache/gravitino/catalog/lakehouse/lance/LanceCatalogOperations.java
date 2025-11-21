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

import static org.apache.gravitino.Entity.EntityType.TABLE;

import com.google.common.collect.Lists;
import com.lancedb.lance.Dataset;
import com.lancedb.lance.WriteParams;
import com.lancedb.lance.index.DistanceType;
import com.lancedb.lance.index.IndexParams;
import com.lancedb.lance.index.IndexType;
import com.lancedb.lance.index.vector.VectorIndexParams;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.catalog.lakehouse.GenericLakehouseTablePropertiesMetadata;
import org.apache.gravitino.catalog.lakehouse.LakehouseCatalogOperations;
import org.apache.gravitino.catalog.lakehouse.LakehouseTableFormat;
import org.apache.gravitino.catalog.lakehouse.utils.EntityConverter;
import org.apache.gravitino.connector.CatalogInfo;
import org.apache.gravitino.connector.GenericLakehouseTable;
import org.apache.gravitino.connector.HasPropertyMetadata;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;
import org.apache.gravitino.lance.common.ops.gravitino.LanceDataTypeConverter;
import org.apache.gravitino.lance.common.utils.LancePropertiesUtils;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.indexes.Indexes.IndexImpl;
import org.apache.gravitino.utils.PrincipalUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LanceCatalogOperations implements LakehouseCatalogOperations {
  private static final Logger LOG = LoggerFactory.getLogger(LanceCatalogOperations.class);

  private EntityStore store;

  @Override
  public void initialize(
      Map<String, String> config, CatalogInfo info, HasPropertyMetadata propertiesMetadata)
      throws RuntimeException {
    store = GravitinoEnv.getInstance().entityStore();
  }

  @Override
  public void testConnection(
      NameIdentifier catalogIdent,
      Catalog.Type type,
      String provider,
      String comment,
      Map<String, String> properties)
      throws Exception {}

  @Override
  public void close() throws IOException {}

  @Override
  public NameIdentifier[] listTables(Namespace namespace) throws NoSuchSchemaException {
    // No need to do nothing here as GenericLakehouseCatalogOperations will do the work.
    throw new UnsupportedOperationException(
        "We should not reach here as we could get table info" + "from metastore directly.");
  }

  @Override
  public Table loadTable(NameIdentifier ident) throws NoSuchTableException {
    // No need to do nothing here as GenericLakehouseCatalogOperations will do the work.
    throw new UnsupportedOperationException(
        "We should not reach here as we could get table info" + "from metastore directly.");
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
    // Ignore partitions, distributions, sortOrders, and indexes for Lance tables;
    String location = properties.get(GenericLakehouseTablePropertiesMetadata.LAKEHOUSE_LOCATION);
    Map<String, String> storageProps = LancePropertiesUtils.getLanceStorageOptions(properties);

    try (Dataset ignored =
        Dataset.create(
            new RootAllocator(),
            location,
            convertColumnsToArrowSchema(columns),
            new WriteParams.Builder().withStorageOptions(storageProps).build())) {
      GenericLakehouseTable.Builder builder = GenericLakehouseTable.builder();
      return builder
          .withName(ident.name())
          .withColumns(columns)
          .withComment(comment)
          .withProperties(properties)
          .withDistribution(distribution)
          .withIndexes(indexes)
          .withAuditInfo(
              AuditInfo.builder()
                  .withCreator(PrincipalUtils.getCurrentUserName())
                  .withCreateTime(Instant.now())
                  .build())
          .withPartitioning(partitions)
          .withSortOrders(sortOrders)
          .withFormat(LakehouseTableFormat.LANCE.lowerName())
          .build();
    }
  }

  private org.apache.arrow.vector.types.pojo.Schema convertColumnsToArrowSchema(Column[] columns) {
    List<Field> fields =
        Arrays.stream(columns)
            .map(
                col ->
                    LanceDataTypeConverter.CONVERTER.toArrowField(
                        col.name(), col.dataType(), col.nullable()))
            .collect(Collectors.toList());
    return new org.apache.arrow.vector.types.pojo.Schema(fields);
  }

  @Override
  public Table alterTable(NameIdentifier ident, TableChange... changes)
      throws NoSuchTableException, IllegalArgumentException {
    // Lance only supports adding indexes for now.
    List<Index> addedIndexes = Lists.newArrayList();
    List<String> droppedColumns = Lists.newArrayList();
    List<TableChange.DeleteColumn> dropColumnChanges = Lists.newArrayList();
    // Only support for adding index for now.
    for (TableChange change : changes) {
      if (change instanceof TableChange.AddIndex addIndexChange) {
        Index index =
            IndexImpl.builder()
                .withIndexType(addIndexChange.getType())
                .withName(addIndexChange.getName())
                .withFieldNames(addIndexChange.getFieldNames())
                .build();
        addedIndexes.add(index);
      } else if (change instanceof TableChange.DeleteColumn deleteColumn) {
        droppedColumns.add(deleteColumn.fieldName()[0]);
        dropColumnChanges.add(deleteColumn);
      } else {
        throw new UnsupportedOperationException(
            "LanceCatalogOperations only support adding index and dropping columns for now.");
      }
    }

    // Check columns to drop already exist in the table.

    TableEntity oldEntity;
    TableEntity newEntity;
    try {
      oldEntity = store.get(ident, Entity.EntityType.TABLE, TableEntity.class);
      for (TableChange.DeleteColumn dropColumnChange : dropColumnChanges) {
        String colName = dropColumnChange.fieldName()[0];
        boolean exist =
            oldEntity.columns().stream().anyMatch(col -> col.name().equalsIgnoreCase(colName));
        if (!exist && !dropColumnChange.getIfExists()) {
          throw new IllegalArgumentException(
              String.format(
                  "Cannot drop column %s as it does not exist in table %s and ifExists is set to false.",
                  colName, ident));
        }
      }

      newEntity =
          TableEntity.builder()
              .withId(oldEntity.id())
              .withName(oldEntity.name())
              .withNamespace(oldEntity.namespace())
              .withAuditInfo(
                  AuditInfo.builder()
                      .withCreator(oldEntity.auditInfo().creator())
                      .withCreateTime(oldEntity.auditInfo().createTime())
                      .withLastModifier(PrincipalUtils.getCurrentPrincipal().getName())
                      .withLastModifiedTime(Instant.now())
                      .build())
              .withColumns(
                  oldEntity.columns().stream()
                      .filter(col -> !droppedColumns.contains(col.name()))
                      .collect(Collectors.toList()))
              .withIndexes(
                  ArrayUtils.addAll(oldEntity.indexes(), addedIndexes.toArray(new Index[0])))
              .withDistribution(oldEntity.distribution())
              .withPartitioning(oldEntity.partitioning())
              .withSortOrders(oldEntity.sortOrders())
              .withProperties(oldEntity.properties())
              .withComment(oldEntity.comment())
              .build();
      store.update(ident, TableEntity.class, TABLE, tableEntity -> newEntity);
    } catch (NoSuchEntityException e) {
      throw new NoSuchTableException("Table does not exist: %s", ident);
    } catch (IOException e) {
      throw new RuntimeException("Failed to load table entity for: " + ident, e);
    }

    try {
      handleTableChange(newEntity, addedIndexes, droppedColumns);
      return GenericLakehouseTable.builder()
          .withFormat(LakehouseTableFormat.LANCE.lowerName())
          .withProperties(newEntity.properties())
          .withAuditInfo(newEntity.auditInfo())
          .withSortOrders(newEntity.sortOrders())
          .withPartitioning(newEntity.partitioning())
          .withDistribution(newEntity.distribution())
          .withColumns(EntityConverter.toColumns(newEntity.columns()))
          .withIndexes(newEntity.indexes())
          .withName(newEntity.name())
          .withComment(newEntity.comment())
          .build();
    } catch (Exception e) {
      try {
        // Rollback to old entity if update fails.
        store.update(ident, TableEntity.class, TABLE, tableEntity -> oldEntity);
      } catch (Exception ex) {
        LOG.error("Failed to rollback table entity for: " + ident, ex);
      }

      if (e.getClass().isAssignableFrom(RuntimeException.class)) {
        throw e;
      }

      throw new RuntimeException("Failed to update table entity for: " + ident, e);
    }
  }

  private void handleTableChange(
      TableEntity updatedEntity, List<Index> addedIndexes, List<String> columnsToDrop) {
    String location =
        updatedEntity.properties().get(GenericLakehouseTablePropertiesMetadata.LAKEHOUSE_LOCATION);
    try (Dataset dataset = Dataset.open(location, new RootAllocator())) {
      // For Lance, we only support adding indexes, so in fact, we can't handle drop index here.
      for (Index index : addedIndexes) {
        IndexType indexType = IndexType.valueOf(index.type().name());
        IndexParams indexParams = getIndexParamsByIndexType(indexType);

        dataset.createIndex(
            Arrays.stream(index.fieldNames())
                .map(fieldPath -> String.join(".", fieldPath))
                .collect(Collectors.toList()),
            indexType,
            Optional.of(index.name()),
            indexParams,
            true);
      }

      if (!columnsToDrop.isEmpty()) {
        dataset.dropColumns(columnsToDrop);
      }
    }
  }

  private IndexParams getIndexParamsByIndexType(IndexType indexType) {
    switch (indexType) {
      case SCALAR:
        return new IndexParams.Builder().build();
      case VECTOR:
        // TODO make these parameters configurable
        int numberOfDimensions = 3; // this value should be determined dynamically based on the data
        // Add properties to Index to set this value.
        return new IndexParams.Builder()
            .setVectorIndexParams(
                VectorIndexParams.ivfPq(2, 8, numberOfDimensions, DistanceType.L2, 2))
            .build();
      default:
        throw new IllegalArgumentException("Unsupported index type: " + indexType);
    }
  }

  @Override
  public boolean purgeTable(NameIdentifier ident) {
    try {
      TableEntity tableEntity = store.get(ident, Entity.EntityType.TABLE, TableEntity.class);
      Map<String, String> lancePropertiesMap = tableEntity.properties();
      String location =
          lancePropertiesMap.get(GenericLakehouseTablePropertiesMetadata.LAKEHOUSE_LOCATION);

      if (!store.delete(ident, Entity.EntityType.TABLE)) {
        throw new RuntimeException("Failed to purge Lance table: " + ident.name());
      }

      // Drop the Lance dataset from cloud storage.
      Dataset.drop(location, LancePropertiesUtils.getLanceStorageOptions(lancePropertiesMap));
      return true;
    } catch (IOException e) {
      throw new RuntimeException("Failed to purge Lance table: " + ident.name(), e);
    }
  }

  @Override
  public boolean dropTable(NameIdentifier ident) {
    // TODO We will handle it in GenericLakehouseCatalogOperations. However, we need
    //  to figure out it's a external table or not first. we will introduce a property
    //  to indicate that. Currently, all Lance tables are external tables. `drop` will
    //  just remove the metadata in metastore and will not delete data in storage.
    throw new UnsupportedOperationException(
        "LanceCatalogOperations does not support dropTable operation.");
  }
}
