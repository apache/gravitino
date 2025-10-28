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

import com.google.common.collect.ImmutableMap;
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
import org.apache.arrow.vector.types.pojo.ArrowType;
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

public class LanceCatalogOperations implements LakehouseCatalogOperations {

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
    try (Dataset ignored =
        Dataset.create(
            new RootAllocator(),
            location,
            convertColumnsToSchema(columns),
            new WriteParams.Builder().build())) {
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

  private org.apache.arrow.vector.types.pojo.Schema convertColumnsToSchema(Column[] columns) {
    LanceDataTypeConverter converter = new LanceDataTypeConverter();
    List<Field> fields =
        Arrays.stream(columns)
            .map(
                col -> {
                  boolean nullable = col.nullable();
                  ArrowType parentType = converter.fromGravitino(col.dataType());
                  List<ArrowType> childTypes = converter.getChildTypes(col.dataType());
                  List<Field> childFields =
                      childTypes.stream()
                          .map(
                              childType ->
                                  new org.apache.arrow.vector.types.pojo.Field(
                                      "",
                                      org.apache.arrow.vector.types.pojo.FieldType.nullable(
                                          childType),
                                      null /* child type is null */))
                          .collect(Collectors.toList());

                  if (nullable) {
                    return new org.apache.arrow.vector.types.pojo.Field(
                        col.name(),
                        org.apache.arrow.vector.types.pojo.FieldType.nullable(parentType),
                        childFields);
                  }

                  // not nullable
                  return new org.apache.arrow.vector.types.pojo.Field(
                      col.name(),
                      org.apache.arrow.vector.types.pojo.FieldType.notNullable(parentType),
                      childFields);
                })
            .collect(Collectors.toList());
    return new org.apache.arrow.vector.types.pojo.Schema(fields);
  }

  @Override
  public Table alterTable(NameIdentifier ident, TableChange... changes)
      throws NoSuchTableException, IllegalArgumentException {
    // Lance only supports adding indexes for now.
    List<Index> addedIndexes = Lists.newArrayList();

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
      }
    }

    TableEntity updatedEntity;
    try {
      TableEntity entity = store.get(ident, Entity.EntityType.TABLE, TableEntity.class);
      updatedEntity =
          store.update(
              ident,
              TableEntity.class,
              TABLE,
              tableEntity ->
                  TableEntity.builder()
                      .withId(tableEntity.id())
                      .withName(tableEntity.name())
                      .withNamespace(tableEntity.namespace())
                      .withFormat(entity.getFormat())
                      .withAuditInfo(
                          AuditInfo.builder()
                              .withCreator(tableEntity.auditInfo().creator())
                              .withCreateTime(tableEntity.auditInfo().createTime())
                              .withLastModifier(PrincipalUtils.getCurrentPrincipal().getName())
                              .withLastModifiedTime(Instant.now())
                              .build())
                      .withColumns(tableEntity.columns())
                      .withIndexes(
                          ArrayUtils.addAll(
                              entity.getIndexes(), addedIndexes.toArray(new Index[0])))
                      .withDistribution(tableEntity.getDistribution())
                      .withPartitions(tableEntity.getPartitions())
                      .withSortOrder(tableEntity.getSortOrder())
                      .withProperties(tableEntity.getProperties())
                      .withComment(tableEntity.getComment())
                      .build());

      // Add indexes to Lance dataset
      addLanceIndex(updatedEntity, addedIndexes);

      // return the updated table
      return GenericLakehouseTable.builder()
          .withFormat(updatedEntity.getFormat())
          .withProperties(updatedEntity.getProperties())
          .withAuditInfo(updatedEntity.auditInfo())
          .withSortOrders(updatedEntity.getSortOrder())
          .withPartitioning(updatedEntity.getPartitions())
          .withDistribution(updatedEntity.getDistribution())
          .withColumns(EntityConverter.toColumns(updatedEntity.columns()))
          .withIndexes(updatedEntity.getIndexes())
          .withName(updatedEntity.name())
          .withComment(updatedEntity.getComment())
          .build();
    } catch (NoSuchEntityException e) {
      throw new NoSuchTableException("No such table: %s", ident);
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to load table entity for: " + ident, ioe);
    }
  }

  private void addLanceIndex(TableEntity updatedEntity, List<Index> addedIndexes) {
    String location =
        updatedEntity
            .getProperties()
            .get(GenericLakehouseTablePropertiesMetadata.LAKEHOUSE_LOCATION);
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
  public boolean dropTable(NameIdentifier ident) {
    try {
      TableEntity tableEntity = store.get(ident, Entity.EntityType.TABLE, TableEntity.class);
      Map<String, String> lancePropertiesMap = tableEntity.getProperties();
      String location =
          lancePropertiesMap.get(GenericLakehouseTablePropertiesMetadata.LAKEHOUSE_LOCATION);

      if (!store.delete(ident, Entity.EntityType.TABLE)) {
        throw new RuntimeException("Failed to drop Lance table: " + ident.name());
      }

      // Drop the Lance dataset from cloud storage.
      Dataset.drop(location, ImmutableMap.of());
      return true;
    } catch (IOException e) {
      throw new RuntimeException("Failed to drop Lance table: " + ident.name(), e);
    }
  }
}
