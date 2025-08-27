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
package org.apache.gravitino.stats.storage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.lancedb.lance.Dataset;
import com.lancedb.lance.Fragment;
import com.lancedb.lance.FragmentMetadata;
import com.lancedb.lance.FragmentOperation;
import com.lancedb.lance.WriteParams;
import com.lancedb.lance.ipc.LanceScanner;
import com.lancedb.lance.ipc.ScanOptions;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.stats.PartitionRange;
import org.apache.gravitino.stats.PartitionStatisticsDrop;
import org.apache.gravitino.stats.PartitionStatisticsModification;
import org.apache.gravitino.stats.PartitionStatisticsUpdate;
import org.apache.gravitino.stats.StatisticValue;
import org.apache.gravitino.utils.MetadataObjectUtil;
import org.apache.gravitino.utils.PrincipalUtils;

/** LancePartitionStatisticStorage is based on Lance format files. */
public class LancePartitionStatisticStorage implements PartitionStatisticStorage {

  private static final String LOCATION = "location";
  private static final String DEFAULT_LOCATION =
      String.join(File.separator, System.getenv("GRAVITINO_HOME"), "data", "lance");
  private static final String MAX_ROWS_PER_FILE = "maxRowsPerFile";
  private static final int DEFAULT_MAX_ROWS_PER_FILE = 1000000; // 10M
  private static final String MAX_BYTES_PER_FILE = "maxBytesPerFile";
  private static final int DEFAULT_MAX_BYTES_PER_FILE = 100 * 1024 * 1024; // 100 MB
  private static final String MAX_ROWS_PER_GROUP = "maxRowsPerGroup";
  private static final int DEFAULT_MAX_ROWS_PER_GROUP = 1000000; // 1M
  private static final String READ_BATCH_SIZE = "readBatchSize";
  private static final int DEFAULT_READ_BATCH_SIZE = 10000; // 10K
  // The schema is `table_id`, `partition_name`,  `statistic_name`, `statistic_value`, `audit_info`
  private static final String TABLE_ID_COLUMN = "table_id";
  private static final String PARTITION_NAME_COLUMN = "partition_name";
  private static final String STATISTIC_NAME_COLUMN = "statistic_name";
  private static final String STATISTIC_VALUE_COLUMN = "statistic_value";
  private static final String AUDIT_INFO_COLUMN = "audit_info";

  private static final Schema SCHEMA =
      new Schema(
          Arrays.asList(
              Field.notNullable(TABLE_ID_COLUMN, new ArrowType.Int(64, false)),
              Field.notNullable(PARTITION_NAME_COLUMN, new ArrowType.Utf8()),
              Field.notNullable(STATISTIC_NAME_COLUMN, new ArrowType.Utf8()),
              Field.notNullable(STATISTIC_VALUE_COLUMN, new ArrowType.LargeUtf8()),
              Field.notNullable(AUDIT_INFO_COLUMN, new ArrowType.Utf8())));

  private final Map<String, String> properties;
  private final String location;
  private final BufferAllocator allocator;
  private final int maxRowsPerFile;
  private final int maxBytesPerFile;
  private final int maxRowsPerGroup;
  private final int readBatchSize;

  private final EntityStore entityStore = GravitinoEnv.getInstance().entityStore();

  public LancePartitionStatisticStorage(Map<String, String> properties) {
    this.allocator = new RootAllocator();
    this.location = properties.getOrDefault(LOCATION, DEFAULT_LOCATION);
    this.maxRowsPerFile =
        Integer.parseInt(
            properties.getOrDefault(MAX_ROWS_PER_FILE, String.valueOf(DEFAULT_MAX_ROWS_PER_FILE)));
    Preconditions.checkArgument(
        maxRowsPerFile > 0, "Lance partition statistics storage maxRowsPerFile must be positive");

    this.maxBytesPerFile =
        Integer.parseInt(
            properties.getOrDefault(
                MAX_BYTES_PER_FILE, String.valueOf(DEFAULT_MAX_BYTES_PER_FILE)));
    Preconditions.checkArgument(
        maxBytesPerFile > 0, "Lance partition statistics storage maxBytesPerFile must be positive");

    this.maxRowsPerGroup =
        Integer.parseInt(
            properties.getOrDefault(
                MAX_ROWS_PER_GROUP, String.valueOf(DEFAULT_MAX_ROWS_PER_GROUP)));
    Preconditions.checkArgument(
        maxRowsPerGroup > 0, "Lance partition statistics storage maxRowsPerGroup must be positive");

    this.readBatchSize =
        Integer.parseInt(
            properties.getOrDefault(READ_BATCH_SIZE, String.valueOf(DEFAULT_READ_BATCH_SIZE)));
    Preconditions.checkArgument(
        readBatchSize > 0, "Lance partition statistics storage readBatchSize must be positive");
    this.properties = properties;
  }

  @Override
  public List<PersistedPartitionStatistics> listStatistics(
      String metalake, MetadataObject metadataObject, PartitionRange partitionRange)
      throws IOException {
    NameIdentifier identifier = MetadataObjectUtil.toEntityIdent(metalake, metadataObject);
    Entity.EntityType type = MetadataObjectUtil.toEntityType(metadataObject);

    Long tableId = entityStore.get(identifier, type, TableEntity.class).id();

    return listStatisticsImpl(tableId, getPartitionFilter(partitionRange));
  }

  @Override
  public int dropStatistics(
      String metalake, List<MetadataObjectStatisticsDrop> partitionStatisticsToDrop)
      throws IOException {
    for (MetadataObjectStatisticsDrop objectDrop : partitionStatisticsToDrop) {
      NameIdentifier identifier =
          MetadataObjectUtil.toEntityIdent(metalake, objectDrop.metadataObject());
      Entity.EntityType type = MetadataObjectUtil.toEntityType(objectDrop.metadataObject());

      Long tableId = entityStore.get(identifier, type, TableEntity.class).id();
      dropStatisticsImpl(tableId, objectDrop.drops());
    }

    // Lance storage can't get the number of dropped statistics, so we return 1 as a placeholder.
    return 1;
  }

  @Override
  public void updateStatistics(
      String metalake, List<MetadataObjectStatisticsUpdate> statisticsToUpdate) throws IOException {
    try {
      //  TODO: The small updates and deletion may cause performance issues. The storage need to add
      // compaction operations.
      for (MetadataObjectStatisticsUpdate objectUpdate : statisticsToUpdate) {
        NameIdentifier identifier =
            MetadataObjectUtil.toEntityIdent(metalake, objectUpdate.metadataObject());
        Entity.EntityType type = MetadataObjectUtil.toEntityType(objectUpdate.metadataObject());

        Long tableId = entityStore.get(identifier, type, TableEntity.class).id();
        List<PartitionStatisticsDrop> partitionDrops =
            objectUpdate.partitionUpdates().stream()
                .map(
                    partitionStatisticsUpdate ->
                        PartitionStatisticsModification.drop(
                            partitionStatisticsUpdate.partitionName(),
                            Lists.newArrayList(partitionStatisticsUpdate.statistics().keySet())))
                .collect(Collectors.toList());

        // TODO: Lance Java API doesn't support the upsert operations although Python API has
        // already supported it. We should push Lance community to support it, otherwise  we can't
        // accomplish update operation in one transaction.
        dropStatisticsImpl(tableId, partitionDrops);
        appendStatisticsImpl(tableId, objectUpdate.partitionUpdates());
      }
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  private void appendStatisticsImpl(Long tableId, List<PartitionStatisticsUpdate> updates) {
    String fileName = getFilePath(tableId);
    try (Dataset datasetRead = open(fileName)) {
      List<FragmentMetadata> fragmentMetas;
      int count = 0;
      try (VectorSchemaRoot root = VectorSchemaRoot.create(SCHEMA, allocator)) {
        for (PartitionStatisticsUpdate update : updates) {
          count += update.statistics().size();
        }

        for (FieldVector vector : root.getFieldVectors()) {
          vector.setInitialCapacity(count);
        }
        root.allocateNew();
        int index = 0;

        for (PartitionStatisticsUpdate updatePartitionStatistic : updates) {
          String partitionName = updatePartitionStatistic.partitionName();
          for (Map.Entry<String, StatisticValue<?>> statistic :
              updatePartitionStatistic.statistics().entrySet()) {
            String statisticName = statistic.getKey();
            String statisticValue =
                JsonUtils.anyFieldMapper().writeValueAsString(statistic.getValue());

            UInt8Vector tableIdVector = (UInt8Vector) root.getVector(TABLE_ID_COLUMN);
            VarCharVector partitionNameVector =
                (VarCharVector) root.getVector(PARTITION_NAME_COLUMN);
            VarCharVector statisticNameVector =
                (VarCharVector) root.getVector(STATISTIC_NAME_COLUMN);
            LargeVarCharVector statisticValueVector =
                (LargeVarCharVector) root.getVector(STATISTIC_VALUE_COLUMN);
            VarCharVector auditInfoVector = (VarCharVector) root.getVector(AUDIT_INFO_COLUMN);

            tableIdVector.set(index, tableId);
            partitionNameVector.setSafe(index, partitionName.getBytes(StandardCharsets.UTF_8));
            statisticNameVector.setSafe(index, statisticName.getBytes(StandardCharsets.UTF_8));
            statisticValueVector.setSafe(index, statisticValue.getBytes(StandardCharsets.UTF_8));
            AuditInfo auditInfo =
                AuditInfo.builder()
                    .withCreator(PrincipalUtils.getCurrentUserName())
                    .withCreateTime(Instant.now())
                    .withLastModifier(PrincipalUtils.getCurrentUserName())
                    .withLastModifiedTime(Instant.now())
                    .build();
            auditInfoVector.setSafe(
                index,
                JsonUtils.anyFieldMapper()
                    .writeValueAsString(auditInfo)
                    .getBytes(StandardCharsets.UTF_8));

            index++;
          }
        }

        root.setRowCount(index);

        fragmentMetas =
            Fragment.create(
                getFilePath(tableId),
                allocator,
                root,
                new WriteParams.Builder()
                    .withMaxRowsPerFile(maxRowsPerFile)
                    .withMaxBytesPerFile(maxBytesPerFile)
                    .withMaxRowsPerGroup(maxRowsPerGroup)
                    .withStorageOptions(properties)
                    .build());
        FragmentOperation.Append appendOp = new FragmentOperation.Append(fragmentMetas);
        Dataset.commit(
                allocator,
                getFilePath(tableId),
                appendOp,
                Optional.of(datasetRead.version()),
                properties)
            .close();
      }

    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize statistic value", e);
    }
  }

  private String getFilePath(Long tableId) {
    return location + "/" + tableId + ".lance";
  }

  private void dropStatisticsImpl(Long tableId, List<PartitionStatisticsDrop> drops) {
    String fileName = getFilePath(tableId);
    try (Dataset dataset = open(fileName)) {
      List<String> partitionSQLs = Lists.newArrayList();
      for (PartitionStatisticsDrop drop : drops) {
        List<String> statistics = drop.statisticNames();
        String partition = drop.partitionName();
        partitionSQLs.add(
            "table_id = "
                + tableId
                + " AND partition_name = '"
                + partition
                + "' AND statistic_name IN ("
                + statistics.stream().map(str -> "'" + str + "'").collect(Collectors.joining(", "))
                + ")");
      }

      if (partitionSQLs.size() == 1) {
        dataset.delete(partitionSQLs.get(0));
      } else if (partitionSQLs.size() > 1) {
        String filterSQL =
            partitionSQLs.stream().map(str -> "(" + str + ")").collect(Collectors.joining(" OR "));
        dataset.delete(filterSQL);
      }
    }
  }

  @Override
  public void close() throws IOException {
    if (allocator != null) {
      allocator.close();
    }
  }

  private static String getPartitionFilter(PartitionRange range) {
    String fromPartitionNameFilter =
        range
            .lowerPartitionName()
            .flatMap(
                name ->
                    range
                        .lowerBoundType()
                        .map(
                            type ->
                                "AND partition_name "
                                    + (type == PartitionRange.BoundType.CLOSED ? ">= " : "> ")
                                    + "'"
                                    + name
                                    + "'"))
            .orElse("");
    String toPartitionNameFilter =
        range
            .upperPartitionName()
            .flatMap(
                name ->
                    range
                        .upperBoundType()
                        .map(
                            type ->
                                "AND partition_name "
                                    + (type == PartitionRange.BoundType.CLOSED ? "<= " : "< ")
                                    + "'"
                                    + name
                                    + "'"))
            .orElse("");

    return fromPartitionNameFilter + toPartitionNameFilter;
  }

  private List<PersistedPartitionStatistics> listStatisticsImpl(
      Long tableId, String partitionFilter) {
    String fileName = getFilePath(tableId);

    try (Dataset dataset = open(fileName)) {

      String filter = "table_id = " + tableId + partitionFilter;

      try (LanceScanner scanner =
          dataset.newScan(
              new ScanOptions.Builder()
                  .columns(
                      Arrays.asList(
                          TABLE_ID_COLUMN,
                          PARTITION_NAME_COLUMN,
                          STATISTIC_NAME_COLUMN,
                          STATISTIC_VALUE_COLUMN,
                          AUDIT_INFO_COLUMN))
                  .withRowId(true)
                  .batchSize(readBatchSize)
                  .filter(filter)
                  .build())) {
        Map<String, List<PersistedStatistic>> partitionStatistics = Maps.newConcurrentMap();
        try (ArrowReader reader = scanner.scanBatches()) {
          while (reader.loadNextBatch()) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            List<FieldVector> fieldVectors = root.getFieldVectors();
            VarCharVector partitionNameVector = (VarCharVector) fieldVectors.get(1);
            VarCharVector statisticNameVector = (VarCharVector) fieldVectors.get(2);
            LargeVarCharVector statisticValueVector = (LargeVarCharVector) fieldVectors.get(3);
            VarCharVector auditInfoNameVector = (VarCharVector) fieldVectors.get(4);

            for (int i = 0; i < root.getRowCount(); i++) {
              String partitionName = new String(partitionNameVector.get(i), StandardCharsets.UTF_8);
              String statisticName = new String(statisticNameVector.get(i), StandardCharsets.UTF_8);
              String statisticValueStr =
                  new String(statisticValueVector.get(i), StandardCharsets.UTF_8);
              String auditInoStr = new String(auditInfoNameVector.get(i), StandardCharsets.UTF_8);

              StatisticValue<?> statisticValue =
                  JsonUtils.anyFieldMapper().readValue(statisticValueStr, StatisticValue.class);
              AuditInfo auditInfo =
                  JsonUtils.anyFieldMapper().readValue(auditInoStr, AuditInfo.class);

              PersistedStatistic persistedStatistic =
                  PersistedStatistic.of(statisticName, statisticValue, auditInfo);

              partitionStatistics
                  .computeIfAbsent(partitionName, k -> Lists.newArrayList())
                  .add(persistedStatistic);
            }
          }

          return partitionStatistics.entrySet().stream()
              .map(entry -> PersistedPartitionStatistics.of(entry.getKey(), entry.getValue()))
              .collect(Collectors.toList());
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  private Dataset open(String fileName) {
    try {
      return Dataset.open(fileName, allocator);
    } catch (IllegalArgumentException illegalArgumentException) {
      if (illegalArgumentException.getMessage().contains("was not found")) {
        return Dataset.create(allocator, fileName, SCHEMA, new WriteParams.Builder().build());
      } else {
        throw illegalArgumentException;
      }
    }
  }
}
