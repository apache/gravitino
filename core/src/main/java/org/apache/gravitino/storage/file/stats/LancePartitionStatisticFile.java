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
package org.apache.gravitino.storage.file.stats;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.lancedb.lance.Dataset;
import com.lancedb.lance.Fragment;
import com.lancedb.lance.FragmentMetadata;
import com.lancedb.lance.FragmentOperation;
import com.lancedb.lance.ReadOptions;
import com.lancedb.lance.WriteParams;
import com.lancedb.lance.ipc.LanceScanner;
import com.lancedb.lance.ipc.ScanOptions;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
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
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.stats.StatisticValue;

public class LancePartitionStatisticFile implements PartitionStatisticFile {

  private final String location;
  private final String format;
  private final BufferAllocator allocator;

  // The schema is `table_id`, `partition_name`,  `statistic_name`, `statistic_value`
  private final String tableIdColumn = "table_id";
  private final String partitionNameColumn = "partition_name";
  private final String statisticNameColumn = "statistic_name";
  private final String statisticValueColumn = "statistic_value";

  private final Schema schema =
      new Schema(
          Arrays.asList(
              Field.notNullable(tableIdColumn, new ArrowType.Int(64, false)),
              Field.notNullable(partitionNameColumn, new ArrowType.Utf8()),
              Field.notNullable(statisticNameColumn, new ArrowType.Utf8()),
              Field.notNullable(statisticValueColumn, new ArrowType.LargeUtf8())));

  public LancePartitionStatisticFile(String location, Map<String, String> properties) {
    this.location = location;
    this.format = "lance"; // Assuming Lance format for simplicity
    this.allocator = new RootAllocator();

    try (Dataset dataset =
        Dataset.create(allocator, location, schema, new WriteParams.Builder().build())) {
      // LOG.debug("Created Lance dataset at location: " + location);
    } catch (Exception e) {
      // Already exists
    }
  }

  @Override
  public String location() {
    return location;
  }

  @Override
  public String format() {
    return format;
  }

  @Override
  public Map<String, Map<String, List<StatisticValue<?>>>> listStatistics(
      long tableId, String fromPartitionName, String toPartitionName) {

    try (Dataset dataset = Dataset.open(location, allocator)) {
      String partitionFilter = "AND partition_name IN ('" + fromPartitionName + "')";
      /*
      String partitionFilter =
          " AND partition_name >= '"
              + fromPartitionName
              + "' AND partition_name < '"
              + toPartitionName
              + "'";*/
      String filter = "table_id = " + tableId + partitionFilter;

      try (LanceScanner scanner =
          dataset.newScan(
              new ScanOptions.Builder()
                  .columns(
                      Arrays.asList(
                          tableIdColumn,
                          partitionNameColumn,
                          statisticNameColumn,
                          statisticValueColumn))
                  .withRowId(true)
                  .filter(filter)
                  .build())) {
        Map<String, Map<String, List<StatisticValue<?>>>> statistics = Maps.newHashMap();
        try (ArrowReader reader = scanner.scanBatches()) {
          while (reader.loadNextBatch()) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            List<FieldVector> fieldVectors = root.getFieldVectors();
            VarCharVector partitionNameVector = (VarCharVector) fieldVectors.get(1);
            VarCharVector statisticNameVector = (VarCharVector) fieldVectors.get(2);
            LargeVarCharVector statisticValueVector = (LargeVarCharVector) fieldVectors.get(3);

            for (int i = 0; i < root.getRowCount(); i++) {
              String partitionName = new String(partitionNameVector.get(i), StandardCharsets.UTF_8);
              String statisticName = new String(statisticNameVector.get(i), StandardCharsets.UTF_8);
              String statisticValueStr =
                  new String(statisticValueVector.get(i), StandardCharsets.UTF_8);

              StatisticValue<?> statisticValue =
                  JsonUtils.anyFieldMapper().readValue(statisticValueStr, StatisticValue.class);

              Map<String, List<StatisticValue<?>>> partitionStats =
                  statistics.computeIfAbsent(partitionName, k -> Maps.newHashMap());
              partitionStats
                  .computeIfAbsent(statisticName, k -> Lists.newArrayList())
                  .add(statisticValue);
            }
          }
        }
        return ImmutableMap.copyOf(statistics);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void updateStatistics(
      Map<Long, Map<String, Map<String, StatisticValue<?>>>> statisticsToUpdate) {
    Map<Long, Map<String, List<String>>> statisticsToDrop = Maps.newHashMap();
    statisticsToUpdate.forEach(
        (key, value) -> {
          Map<String, List<String>> tableStatistics =
              statisticsToDrop.computeIfAbsent(key, k -> Maps.newHashMap());
          value.forEach(
              (partition, statistics) -> {
                List<String> partitionStatsToDrop =
                    tableStatistics.computeIfAbsent(partition, k -> Lists.newArrayList());
                for (Map.Entry<String, StatisticValue<?>> entry : statistics.entrySet()) {
                  String statisticName = entry.getKey();
                  partitionStatsToDrop.add(statisticName);
                }
              });
          statisticsToDrop.put(key, tableStatistics);
        });
    // dropStatistics(statisticsToDrop);

    try (Dataset datasetRead =
        Dataset.open(allocator, location, new ReadOptions.Builder().build())) {
      List<FragmentMetadata> fragmentMetas;
      int count = 0;
      try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
        for (Map.Entry<Long, Map<String, Map<String, StatisticValue<?>>>> tableStatistic :
            statisticsToUpdate.entrySet()) {
          for (Map.Entry<String, Map<String, StatisticValue<?>>> updatePartitionStatistic :
              tableStatistic.getValue().entrySet()) {
            count += updatePartitionStatistic.getValue().size();
          }
        }

        for (FieldVector vector : root.getFieldVectors()) {
          vector.setInitialCapacity(count);
        }
        root.allocateNew();
        int index = 0;
        for (Map.Entry<Long, Map<String, Map<String, StatisticValue<?>>>> tableStatistic :
            statisticsToUpdate.entrySet()) {
          Long tableId = tableStatistic.getKey();
          for (Map.Entry<String, Map<String, StatisticValue<?>>> updatePartitionStatistic :
              tableStatistic.getValue().entrySet()) {
            String partitionName = updatePartitionStatistic.getKey();
            for (Map.Entry<String, StatisticValue<?>> statistic :
                updatePartitionStatistic.getValue().entrySet()) {
              String statisticName = statistic.getKey();
              String statisticValue =
                  JsonUtils.anyFieldMapper().writeValueAsString(statistic.getValue());

              UInt8Vector tableIdVector = (UInt8Vector) root.getVector(tableIdColumn);
              VarCharVector partitionNameVector =
                  (VarCharVector) root.getVector(partitionNameColumn);
              VarCharVector statisticNameVector =
                  (VarCharVector) root.getVector(statisticNameColumn);
              LargeVarCharVector statisticValueVector =
                  (LargeVarCharVector) root.getVector(statisticValueColumn);

              tableIdVector.set(index, tableId);
              partitionNameVector.setSafe(index, partitionName.getBytes(StandardCharsets.UTF_8));
              statisticNameVector.setSafe(index, statisticName.getBytes(StandardCharsets.UTF_8));
              statisticValueVector.setSafe(index, statisticValue.getBytes(StandardCharsets.UTF_8));

              index++;
            }
          }
        }

        root.setRowCount(index);

        fragmentMetas =
            Fragment.create(
                location,
                allocator,
                root,
                new WriteParams.Builder()
                    .withMaxRowsPerFile(Integer.MAX_VALUE)
                    .withMaxBytesPerFile(Integer.MAX_VALUE)
                    .withMaxRowsPerGroup(Integer.MAX_VALUE)
                    .build());
      } catch (JsonProcessingException e) {
        throw new RuntimeException("Failed to serialize statistic value", e);
      }

      FragmentOperation.Append appendOp = new FragmentOperation.Append(fragmentMetas);
      Dataset.commit(
              allocator,
              location,
              appendOp,
              java.util.Optional.of(datasetRead.version()),
              Maps.newHashMap())
          .close();
    }
  }

  @Override
  public void dropStatistics(Map<Long, Map<String, List<String>>> partitionStatisticsToDrop) {
    try (Dataset dataset = Dataset.open(location, allocator)) {
      List<String> partitionSQLs = Lists.newArrayList();
      for (Map.Entry<Long, Map<String, List<String>>> entry :
          partitionStatisticsToDrop.entrySet()) {
        Long tableId = entry.getKey();
        for (Map.Entry<String, List<String>> internalEntry : entry.getValue().entrySet()) {
          List<String> statistics = internalEntry.getValue();
          String partition = internalEntry.getKey();
          partitionSQLs.add(
              "table_id = "
                  + tableId
                  + " AND partition_name = '"
                  + partition
                  + "' AND statistic_name IN ("
                  + statistics.stream()
                      .map(str -> "'" + str + "'")
                      .collect(Collectors.joining(", "))
                  + ")");
        }
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
}
