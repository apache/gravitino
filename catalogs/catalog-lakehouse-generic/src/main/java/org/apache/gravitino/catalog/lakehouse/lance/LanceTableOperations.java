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

import static org.apache.gravitino.lance.common.utils.LanceConstants.LANCE_INDEX_CONFIG_KEY;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.lancedb.lance.Dataset;
import com.lancedb.lance.WriteParams;
import com.lancedb.lance.index.DistanceType;
import com.lancedb.lance.index.IndexParams;
import com.lancedb.lance.index.IndexType;
import com.lancedb.lance.index.scalar.ScalarIndexParams;
import com.lancedb.lance.index.vector.HnswBuildParams;
import com.lancedb.lance.index.vector.IvfBuildParams;
import com.lancedb.lance.index.vector.PQBuildParams;
import com.lancedb.lance.index.vector.SQBuildParams;
import com.lancedb.lance.index.vector.VectorIndexParams;
import com.lancedb.lance.namespace.model.CreateTableIndexRequest;
import com.lancedb.lance.namespace.model.CreateTableIndexRequest.MetricTypeEnum;
import com.lancedb.lance.namespace.util.JsonUtil;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.ManagedSchemaOperations;
import org.apache.gravitino.catalog.ManagedTableOperations;
import org.apache.gravitino.connector.SupportsSchemas;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;
import org.apache.gravitino.lance.common.ops.gravitino.LanceDataTypeConverter;
import org.apache.gravitino.lance.common.utils.LancePropertiesUtils;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.indexes.Indexes;
import org.apache.gravitino.storage.IdGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LanceTableOperations extends ManagedTableOperations {
  private static final Logger LOG = LoggerFactory.getLogger(LanceTableOperations.class);

  private final EntityStore store;

  private final ManagedSchemaOperations schemaOps;

  private final IdGenerator idGenerator;

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
    Map<String, String> storageProps = LancePropertiesUtils.getLanceStorageOptions(properties);

    boolean register =
        Optional.ofNullable(properties.get(LanceTableDelegator.PROPERTY_LANCE_TABLE_REGISTER))
            .map(Boolean::parseBoolean)
            .orElse(false);
    if (register) {
      // If this is a registration operation, just create the table metadata without creating a new
      // dataset
      return super.createTable(
          ident, columns, comment, properties, partitions, distribution, sortOrders, indexes);
    }

    try (Dataset ignored =
        Dataset.create(
            new RootAllocator(),
            location,
            convertColumnsToArrowSchema(columns),
            new WriteParams.Builder().withStorageOptions(storageProps).build())) {
      // Only create the table metadata in Gravitino after the Lance dataset is successfully
      // created.
      return super.createTable(
          ident, columns, comment, properties, partitions, distribution, sortOrders, indexes);
    } catch (NoSuchSchemaException e) {
      throw e;
    } catch (TableAlreadyExistsException e) {
      // If the table metadata already exists, but the underlying lance table was just created
      // successfully, we need to clean up the created lance table to avoid orphaned datasets.
      Dataset.drop(location, LancePropertiesUtils.getLanceStorageOptions(properties));
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

  @Override
  public Table alterTable(NameIdentifier ident, TableChange... changes)
      throws NoSuchSchemaException, TableAlreadyExistsException {
    // Lance only supports adding indexes for now.
    boolean onlyAddIndex =
        Arrays.stream(changes).allMatch(change -> change instanceof TableChange.AddIndex);
    Preconditions.checkArgument(onlyAddIndex, "Only adding indexes is supported for Lance tables");

    List<Index> addedIndexes =
        Arrays.stream(changes)
            .filter(change -> change instanceof TableChange.AddIndex)
            .map(
                change -> {
                  TableChange.AddIndex addIndexChange = (TableChange.AddIndex) change;
                  return Indexes.IndexImpl.builder()
                      .withIndexType(addIndexChange.getType())
                      .withName(addIndexChange.getName())
                      .withFieldNames(addIndexChange.getFieldNames())
                      .withProperties(addIndexChange.getProperties())
                      .build();
                })
            .collect(Collectors.toList());

    Table loadedTable = super.loadTable(ident);

    String location = loadedTable.properties().get(Table.PROPERTY_LOCATION);
    List<Index> addedIndex = addLanceIndex(location, addedIndexes);

    // Since Lance supports adding indexes without an index name, and it will generate index name
    // automatically, we need to modify the TableChange to include the index name after adding the
    // index to Lance dataset.
    changes = modifyAddIndex(changes, addedIndex);
    // After adding the index to the Lance dataset, we need to update the table metadata in
    // Gravitino. If there's any failure during this process, the code will throw an exception
    // and the update won't be applied in Gravitino.
    return super.alterTable(ident, changes);
  }

  @Override
  public boolean purgeTable(NameIdentifier ident) {
    try {
      Table table = loadTable(ident);
      String location = table.properties().get(Table.PROPERTY_LOCATION);

      boolean purged = super.purgeTable(ident);
      // If the table metadata is purged successfully, we can delete the Lance dataset.
      // Otherwise, we should not delete the dataset.
      if (purged) {
        // Delete the Lance dataset at the location
        Dataset.drop(location, LancePropertiesUtils.getLanceStorageOptions(table.properties()));
        LOG.info("Deleted Lance dataset at location {}", location);
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
      Table table = loadTable(ident);
      boolean external =
          Optional.ofNullable(table.properties().get(Table.PROPERTY_EXTERNAL))
              .map(Boolean::parseBoolean)
              .orElse(false);

      boolean dropped = super.dropTable(ident);
      if (external) {
        return dropped;
      }

      // If the table metadata is dropped successfully, and the table is not external, we can delete
      // the
      // Lance dataset. Otherwise, we should not delete the dataset.
      if (dropped) {
        String location = table.properties().get(Table.PROPERTY_LOCATION);

        // Delete the Lance dataset at the location
        Dataset.drop(location, LancePropertiesUtils.getLanceStorageOptions(table.properties()));
        LOG.info("Deleted Lance dataset at location {}", location);
      }

      return dropped;

    } catch (NoSuchTableException e) {
      return false;
    } catch (Exception e) {
      throw new RuntimeException("Failed to drop Lance dataset for table " + ident, e);
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

  private TableChange[] modifyAddIndex(TableChange[] tableChanges, List<Index> addIndex) {
    int indexCount = 0;
    for (int i = 0; i < tableChanges.length; i++) {
      TableChange change = tableChanges[i];
      if (change instanceof TableChange.AddIndex) {
        Index index = addIndex.get(indexCount++);
        tableChanges[i] =
            new TableChange.AddIndex(
                index.type(), index.name(), index.fieldNames(), index.properties());
      }
    }

    return tableChanges;
  }

  private List<Index> addLanceIndex(String location, List<Index> addedIndexes) {
    List<Index> newIndexes = Lists.newArrayList();
    try (RootAllocator rootAllocator = new RootAllocator();
        Dataset dataset = Dataset.open(location, rootAllocator)) {
      for (Index index : addedIndexes) {
        IndexType indexType = getIndexType(index);
        IndexParams indexParams = generateIndexParams(index);
        dataset.createIndex(
            Arrays.stream(index.fieldNames())
                .map(fieldPath -> String.join(".", fieldPath))
                .collect(Collectors.toList()),
            indexType,
            Optional.ofNullable(index.name()),
            indexParams,
            false);

        // Currently lance only supports single-field indexes, so we can use the first field name.
        // Another point is that we need to ensure the index name is not null in Gravitino, so we
        // generate a name if it's null as Lance will generate a name automatically.
        String lanceIndexName =
            index.name() == null ? index.fieldNames()[0][0] + "_idx" : index.name();
        newIndexes.add(
            Indexes.of(index.type(), lanceIndexName, index.fieldNames(), index.properties()));
      }

      return newIndexes;
    }
  }

  private IndexType getIndexType(Index index) {
    IndexType indexType = IndexType.valueOf(index.type().name());
    return switch (indexType) {
        // API only supports these index types for now, but there are more index types in Lance.
      case SCALAR, BTREE, INVERTED, BITMAP -> indexType;
        // According to real test, we need to map IVF_SQ/IVF_PQ/IVF_HNSW_SQ to VECTOR type in Lance,
        // or it will throw exception. For more, please refer to
        // https://github.com/lancedb/lance/issues/5182#issuecomment-3524372490
      case IVF_FLAT, IVF_PQ, IVF_HNSW_SQ -> IndexType.VECTOR;
      default -> throw new IllegalArgumentException("Unsupported index type: " + indexType);
    };
  }

  private IndexParams generateIndexParams(Index index) {
    IndexType indexType = IndexType.valueOf(index.type().name());

    String configJson = index.properties().get(LANCE_INDEX_CONFIG_KEY);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(configJson),
        "Lance index config must be provided in index properties with key %s",
        LANCE_INDEX_CONFIG_KEY);
    CreateTableIndexRequest request;
    try {
      request = JsonUtil.mapper().readValue(configJson, CreateTableIndexRequest.class);
    } catch (Exception e) {
      throw new IllegalArgumentException("Lance index config is invalid", e);
    }

    IndexParams.Builder builder = IndexParams.builder();
    switch (indexType) {
      case SCALAR, BTREE, INVERTED, BITMAP -> builder.setScalarIndexParams(
          ScalarIndexParams.create(indexType.name()));

      case IVF_FLAT -> builder.setVectorIndexParams(
          new VectorIndexParams.Builder(new IvfBuildParams.Builder().build())
              .setDistanceType(toLanceDistanceType(request.getMetricType()))
              .build());
      case IVF_PQ -> builder.setVectorIndexParams(
          new VectorIndexParams.Builder(new IvfBuildParams.Builder().build())
              .setDistanceType(toLanceDistanceType(request.getMetricType()))
              .setPqParams(
                  new PQBuildParams.Builder()
                      .setNumSubVectors(1) // others use default value.
                      .build())
              .build());

      case IVF_SQ -> builder.setVectorIndexParams(
          new VectorIndexParams.Builder(new IvfBuildParams.Builder().build())
              .setDistanceType(toLanceDistanceType(request.getMetricType()))
              .setSqParams(new SQBuildParams.Builder().build())
              .build());

      case IVF_HNSW_SQ -> builder.setVectorIndexParams(
          new VectorIndexParams.Builder(new IvfBuildParams.Builder().build())
              .setDistanceType(toLanceDistanceType(request.getMetricType()))
              .setHnswParams(new HnswBuildParams.Builder().build())
              .build());

      case IVF_HNSW_PQ -> builder.setVectorIndexParams(
          new VectorIndexParams.Builder(new IvfBuildParams.Builder().build())
              .setDistanceType(toLanceDistanceType(request.getMetricType()))
              .setHnswParams(new HnswBuildParams.Builder().build())
              .setPqParams(
                  new PQBuildParams.Builder()
                      .setNumSubVectors(1) // others use default value.
                      .build())
              .build());
      default -> throw new IllegalArgumentException("Unsupported index type: " + indexType);
    }

    return builder.build();
  }

  private DistanceType toLanceDistanceType(MetricTypeEnum metricTypeEnum) {
    if (metricTypeEnum == null) {
      // Default to L2
      return DistanceType.L2;
    }
    String metricName = metricTypeEnum.name();
    for (DistanceType distanceType : DistanceType.values()) {
      if (distanceType.name().equalsIgnoreCase(metricName)) {
        return distanceType;
      }
    }

    throw new IllegalArgumentException("Unsupported metric type: " + metricTypeEnum);
  }
}
