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

import static org.apache.gravitino.catalog.lakehouse.GenericLakehouseTablePropertiesMetadata.LANCE_TABLE_STORAGE_OPTION_PREFIX;
import static org.apache.gravitino.catalog.lakehouse.GenericLakehouseTablePropertiesMetadata.LOCATION;

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
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.catalog.lakehouse.LakehouseCatalogOperations;
import org.apache.gravitino.connector.CatalogInfo;
import org.apache.gravitino.connector.GenericLakehouseTable;
import org.apache.gravitino.connector.HasPropertyMetadata;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;
import org.apache.gravitino.lance.common.ops.gravitino.LanceDataTypeConverter;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.GenericTableEntity;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.GenericTable;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.indexes.Indexes.IndexImpl;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class LanceCatalogOperations implements LakehouseCatalogOperations {

  private Map<String, String> lancePropertiesMap;

  @Override
  public void initialize(
      Map<String, String> config, CatalogInfo info, HasPropertyMetadata propertiesMetadata)
      throws RuntimeException {
    lancePropertiesMap = ImmutableMap.copyOf(config);
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
    return new NameIdentifier[0];
  }

  @Override
  public Table loadTable(NameIdentifier ident) throws NoSuchTableException {
    // Should not come here.
    return null;
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
    String location = properties.get(LOCATION);
    Map<String, String> storageProps =
        properties.entrySet().stream()
            .filter(e -> e.getKey().startsWith(LANCE_TABLE_STORAGE_OPTION_PREFIX))
            .collect(
                Collectors.toMap(
                    e -> e.getKey().substring(LANCE_TABLE_STORAGE_OPTION_PREFIX.length()),
                    Map.Entry::getValue));
    try (Dataset dataset =
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
          .withFormat("lance")
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

    EntityStore entityStore = GravitinoEnv.getInstance().entityStore();
    GenericTableEntity entity;
    try {
      entity = entityStore.get(ident, Entity.EntityType.TABLE, GenericTableEntity.class);
    } catch (NoSuchEntityException e) {
      throw new NoSuchTableException("No such table: %s", ident);
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to load table entity for: " + ident, ioe);
    }

    String location = entity.getProperties().get("location");
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
    } catch (Exception e) {
      throw new RuntimeException("Failed to alter Lance table: " + ident, e);
    }

    GenericTable oldTable = entity.toGenericTable();
    Index[] newIndexes = oldTable.index();
    for (Index index : addedIndexes) {
      newIndexes = ArrayUtils.add(newIndexes, index);
    }

    return GenericLakehouseTable.builder()
        .withFormat(oldTable.format())
        .withProperties(oldTable.properties())
        .withAuditInfo((AuditInfo) oldTable.auditInfo())
        .withSortOrders(oldTable.sortOrder())
        .withPartitioning(oldTable.partitioning())
        .withDistribution(oldTable.distribution())
        .withColumns(oldTable.columns())
        .withIndexes(newIndexes)
        .withName(oldTable.name())
        .withComment(oldTable.comment())
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

  @Override
  public boolean dropTable(NameIdentifier ident) {
    try {
      String location = lancePropertiesMap.get("location");
      // Remove the directory on storage
      FileSystem fs = FileSystem.get(new Configuration());
      return fs.delete(new Path(location), true);
    } catch (IOException e) {
      throw new RuntimeException("Failed to drop Lance table: " + ident.name(), e);
    }
  }
}
