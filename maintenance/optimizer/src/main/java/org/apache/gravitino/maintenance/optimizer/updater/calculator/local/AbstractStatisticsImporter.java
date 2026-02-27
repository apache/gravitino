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

package org.apache.gravitino.maintenance.optimizer.updater.calculator.local;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectReader;
import com.google.common.collect.ImmutableList;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionEntry;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionPath;
import org.apache.gravitino.maintenance.optimizer.api.common.StatisticEntry;
import org.apache.gravitino.maintenance.optimizer.api.common.TableAndPartitionStatistics;
import org.apache.gravitino.maintenance.optimizer.common.PartitionEntryImpl;
import org.apache.gravitino.maintenance.optimizer.common.StatisticEntryImpl;
import org.apache.gravitino.maintenance.optimizer.common.util.IdentifierUtils;
import org.apache.gravitino.stats.StatisticValue;
import org.apache.gravitino.stats.StatisticValues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * partition schema: { "identifier": "schema.table", "stats-type": "partition", "partition-path":
 * {"p1": "v1", "p2": "v2"}, "stats1":"100"}
 *
 * <p>table schema: { "identifier": "schema.table", "stats-type": "table", "stats1":"100"}
 *
 * <p>job schema: { "identifier": "schema.job", "stats-type": "job", "stats1":"100"}
 *
 * <p>For Iceberg we don't have restrict on partition name, but prefer to use the name in Iceberg
 * transform name.
 *
 * <ul>
 *   <li>identity(col) -> col
 *   <li>year(col) -> col_year
 *   <li>month(col) -> col_month
 *   <li>day(col) / days(col) -> col_day
 *   <li>hour(col) / hours(col) -> col_hour
 *   <li>bucket(N, col) -> col_bucket_N
 *   <li>truncate(W, col) -> col_trunc
 * </ul>
 */
abstract class AbstractStatisticsImporter implements StatisticsImporter {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractStatisticsImporter.class);
  private static final ObjectReader RECORD_READER =
      JsonUtils.anyFieldMapper().readerFor(StatisticsRecord.class);

  private final String defaultCatalogName;

  AbstractStatisticsImporter(String defaultCatalogName) {
    this.defaultCatalogName = defaultCatalogName;
  }

  @Override
  public TableAndPartitionStatistics readTableStatistics(NameIdentifier tableIdentifier) {
    return aggregateTableAndPartitionStatistics(tableIdentifier);
  }

  @Override
  public Map<NameIdentifier, TableAndPartitionStatistics> bulkReadAllTableStatistics() {
    return aggregateAllTableAndPartitionStatistics();
  }

  @Override
  public List<StatisticEntry<?>> readJobStatistics(NameIdentifier jobIdentifier) {
    return toStatisticEntries(aggregateJobStatistics(jobIdentifier).get(jobIdentifier));
  }

  @Override
  public Map<NameIdentifier, List<StatisticEntry<?>>> bulkReadAllJobStatistics() {
    return toIdentifierStatisticEntries(aggregateJobStatistics(null));
  }

  private List<StatisticEntry<?>> toStatisticEntries(Map<String, StatisticValue<?>> statsByName) {
    if (statsByName == null || statsByName.isEmpty()) {
      return ImmutableList.of();
    }

    List<StatisticEntry<?>> statistics = new ArrayList<>();
    statsByName.forEach((name, value) -> statistics.add(new StatisticEntryImpl<>(name, value)));
    return ImmutableList.copyOf(statistics);
  }

  private Map<PartitionPath, List<StatisticEntry<?>>> toPartitionStatisticEntries(
      Map<PartitionPath, Map<String, StatisticValue<?>>> statsByPartition) {
    if (statsByPartition == null || statsByPartition.isEmpty()) {
      return Map.of();
    }

    Map<PartitionPath, List<StatisticEntry<?>>> result = new LinkedHashMap<>();
    statsByPartition.forEach(
        (partitionPath, statsByName) -> result.put(partitionPath, toStatisticEntries(statsByName)));
    return result;
  }

  private Map<NameIdentifier, List<StatisticEntry<?>>> toIdentifierStatisticEntries(
      Map<NameIdentifier, Map<String, StatisticValue<?>>> statsByIdentifier) {
    Map<NameIdentifier, List<StatisticEntry<?>>> result = new LinkedHashMap<>();
    if (statsByIdentifier == null || statsByIdentifier.isEmpty()) {
      return result;
    }

    statsByIdentifier.forEach(
        (identifier, statsByName) -> result.put(identifier, toStatisticEntries(statsByName)));
    return result;
  }

  private Map<NameIdentifier, Map<String, StatisticValue<?>>> aggregateStatistics(
      NameIdentifier targetIdentifier) {
    Map<NameIdentifier, Map<String, StatisticValue<?>>> aggregated = new LinkedHashMap<>();
    visitParsedRecords(
        new StatisticsRecordVisitor() {
          @Override
          public void onTable(StatisticsRecord record) {
            NameIdentifier identifier = parseTableIdentifier(record.identifier());
            if (identifier == null) {
              return;
            }
            if (targetIdentifier != null && !targetIdentifier.equals(identifier)) {
              return;
            }

            Map<String, StatisticValue<?>> statisticsByName =
                aggregated.computeIfAbsent(identifier, k -> new LinkedHashMap<>());
            populateStatistics(record, statisticsByName);
          }
        });

    return aggregated;
  }

  private TableAndPartitionStatistics aggregateTableAndPartitionStatistics(
      NameIdentifier targetIdentifier) {
    if (targetIdentifier == null) {
      return new TableAndPartitionStatistics(ImmutableList.of(), Map.of());
    }

    Map<String, StatisticValue<?>> tableStatistics = new LinkedHashMap<>();
    Map<PartitionPath, Map<String, StatisticValue<?>>> partitionStatistics = new LinkedHashMap<>();
    visitParsedRecords(
        new StatisticsRecordVisitor() {
          @Override
          public void onTable(StatisticsRecord record) {
            NameIdentifier identifier = parseTableIdentifier(record.identifier());
            if (targetIdentifier.equals(identifier)) {
              populateStatistics(record, tableStatistics);
            }
          }

          @Override
          public void onPartition(StatisticsRecord record) {
            NameIdentifier identifier = parseTableIdentifier(record.identifier());
            if (!targetIdentifier.equals(identifier)) {
              return;
            }

            Optional<PartitionPath> partitionPathOpt = parsePartitionPath(record.partitionPath());
            if (partitionPathOpt.isEmpty()) {
              return;
            }

            Map<String, StatisticValue<?>> partitionStatsByName =
                partitionStatistics.computeIfAbsent(
                    partitionPathOpt.get(), k -> new LinkedHashMap<>());
            populateStatistics(record, partitionStatsByName);
          }
        });

    return new TableAndPartitionStatistics(
        toStatisticEntries(tableStatistics), toPartitionStatisticEntries(partitionStatistics));
  }

  private Map<NameIdentifier, TableAndPartitionStatistics>
      aggregateAllTableAndPartitionStatistics() {
    Map<NameIdentifier, Map<String, StatisticValue<?>>> tableStatisticsByIdentifier =
        new LinkedHashMap<>();
    Map<NameIdentifier, Map<PartitionPath, Map<String, StatisticValue<?>>>>
        partitionStatisticsByIdentifier = new LinkedHashMap<>();
    visitParsedRecords(
        new StatisticsRecordVisitor() {
          @Override
          public void onTable(StatisticsRecord record) {
            NameIdentifier identifier = parseTableIdentifier(record.identifier());
            if (identifier == null) {
              return;
            }
            Map<String, StatisticValue<?>> tableStats =
                tableStatisticsByIdentifier.computeIfAbsent(identifier, k -> new LinkedHashMap<>());
            populateStatistics(record, tableStats);
          }

          @Override
          public void onPartition(StatisticsRecord record) {
            NameIdentifier identifier = parseTableIdentifier(record.identifier());
            if (identifier == null) {
              return;
            }

            Optional<PartitionPath> partitionPathOpt = parsePartitionPath(record.partitionPath());
            if (partitionPathOpt.isEmpty()) {
              return;
            }

            Map<PartitionPath, Map<String, StatisticValue<?>>> partitionStatsByPath =
                partitionStatisticsByIdentifier.computeIfAbsent(
                    identifier, k -> new LinkedHashMap<>());
            Map<String, StatisticValue<?>> partitionStatsByName =
                partitionStatsByPath.computeIfAbsent(
                    partitionPathOpt.get(), k -> new LinkedHashMap<>());
            populateStatistics(record, partitionStatsByName);
          }
        });

    Map<NameIdentifier, TableAndPartitionStatistics> bundles = new LinkedHashMap<>();
    tableStatisticsByIdentifier.forEach(
        (identifier, tableStats) ->
            bundles.put(
                identifier,
                new TableAndPartitionStatistics(
                    toStatisticEntries(tableStats),
                    toPartitionStatisticEntries(partitionStatisticsByIdentifier.get(identifier)))));
    partitionStatisticsByIdentifier.forEach(
        (identifier, partitionStats) ->
            bundles.putIfAbsent(
                identifier,
                new TableAndPartitionStatistics(
                    ImmutableList.of(), toPartitionStatisticEntries(partitionStats))));
    return bundles;
  }

  private Map<NameIdentifier, Map<String, StatisticValue<?>>> aggregateJobStatistics(
      NameIdentifier targetIdentifier) {
    Map<NameIdentifier, Map<String, StatisticValue<?>>> aggregated = new LinkedHashMap<>();
    visitParsedRecords(
        new StatisticsRecordVisitor() {
          @Override
          public void onJob(StatisticsRecord record) {
            NameIdentifier identifier = parseJobIdentifier(record.identifier());
            if (identifier == null) {
              return;
            }
            if (targetIdentifier != null && !targetIdentifier.equals(identifier)) {
              return;
            }

            Map<String, StatisticValue<?>> statisticsByName =
                aggregated.computeIfAbsent(identifier, k -> new LinkedHashMap<>());
            populateStatistics(record, statisticsByName);
          }
        });

    return aggregated;
  }

  protected abstract BufferedReader openReader() throws IOException;

  private void forEachParsedRecord(Consumer<StatisticsRecord> recordConsumer) {
    try (BufferedReader reader = openReader()) {
      String line;
      while ((line = reader.readLine()) != null) {
        if (StringUtils.isBlank(line)) {
          continue;
        }

        StatisticsRecord record = parseRecord(line);
        if (record == null) {
          continue;
        }
        recordConsumer.accept(record);
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to read statistics", e);
    }
  }

  private void visitParsedRecords(StatisticsRecordVisitor visitor) {
    forEachParsedRecord(record -> dispatchRecordByStatisticsType(record, visitor));
  }

  private void dispatchRecordByStatisticsType(
      StatisticsRecord record, StatisticsRecordVisitor visitor) {
    Optional<StatisticsType> statisticsType = StatisticsType.from(record.statisticsType());
    if (statisticsType.isEmpty()) {
      return;
    }

    switch (statisticsType.get()) {
      case TABLE:
        visitor.onTable(record);
        break;
      case PARTITION:
        visitor.onPartition(record);
        break;
      case JOB:
        visitor.onJob(record);
        break;
    }
  }

  private StatisticsRecord parseRecord(String line) {
    try {
      return RECORD_READER.readValue(line);
    } catch (IOException e) {
      LOG.warn("Skip malformed statistics line: {}", line, e);
      return null;
    }
  }

  private void populateStatistics(
      StatisticsRecord record, Map<String, StatisticValue<?>> statisticsByName) {
    record
        .statisticsFields()
        .forEach(
            (fieldName, node) -> {
              StatisticValue<?> value = parseStatisticValue(fieldName, node);
              if (value != null) {
                statisticsByName.put(fieldName, value);
              }
            });
  }

  private interface StatisticsRecordVisitor {
    default void onTable(StatisticsRecord record) {}

    default void onPartition(StatisticsRecord record) {}

    default void onJob(StatisticsRecord record) {}
  }

  private enum FieldName {
    STATISTICS_TYPE("stats-type"),
    IDENTIFIER("identifier"),
    PARTITION_PATH("partition-path");

    private final String key;

    FieldName(String key) {
      this.key = key;
    }

    private String key() {
      return key;
    }

    private static final Set<String> RESERVED_FIELDS =
        Set.of(STATISTICS_TYPE.key, IDENTIFIER.key, PARTITION_PATH.key);

    private static boolean isReserved(String fieldName) {
      return RESERVED_FIELDS.contains(fieldName);
    }
  }

  private enum StatisticsType {
    TABLE("table"),
    PARTITION("partition"),
    JOB("job");

    private final String type;

    StatisticsType(String type) {
      this.type = type;
    }

    private static Optional<StatisticsType> from(String statisticsTypeValue) {
      if (StringUtils.isBlank(statisticsTypeValue)) {
        return Optional.empty();
      }

      String normalizedType = statisticsTypeValue.toLowerCase(Locale.ROOT);
      for (StatisticsType value : values()) {
        if (value.type.equals(normalizedType)) {
          return Optional.of(value);
        }
      }
      return Optional.empty();
    }
  }

  private NameIdentifier parseTableIdentifier(String identifierText) {
    if (StringUtils.isBlank(identifierText)) {
      return null;
    }
    NameIdentifier parsed =
        IdentifierUtils.parseTableIdentifier(identifierText, defaultCatalogName);
    if (parsed == null) {
      LOG.warn("Skip line with invalid identifier: {}", identifierText);
    }
    return parsed;
  }

  private NameIdentifier parseJobIdentifier(String identifierText) {
    NameIdentifier parsed = IdentifierUtils.parseJobIdentifier(identifierText);
    if (parsed == null && StringUtils.isNotBlank(identifierText)) {
      LOG.warn("Skip line with invalid identifier: {}", identifierText);
    }
    return parsed;
  }

  private static final class StatisticsRecord {
    @JsonProperty("stats-type")
    private String statisticsType;

    @JsonProperty("identifier")
    private String identifier;

    @JsonProperty("partition-path")
    private JsonNode partitionPath;

    private final Map<String, JsonNode> statisticsFields = new LinkedHashMap<>();

    @JsonAnySetter
    void putField(String fieldName, JsonNode fieldValue) {
      if (!FieldName.isReserved(fieldName)) {
        statisticsFields.put(fieldName, fieldValue);
      }
    }

    String statisticsType() {
      return statisticsType;
    }

    String identifier() {
      return identifier;
    }

    JsonNode partitionPath() {
      return partitionPath;
    }

    Map<String, JsonNode> statisticsFields() {
      return statisticsFields;
    }
  }

  private Optional<PartitionPath> parsePartitionPath(JsonNode partitionPathNode) {
    if (partitionPathNode == null || !partitionPathNode.isObject()) {
      return Optional.empty();
    }

    List<PartitionEntry> entries = new ArrayList<>();
    Iterator<Map.Entry<String, JsonNode>> iterator = partitionPathNode.fields();
    while (iterator.hasNext()) {
      Map.Entry<String, JsonNode> entry = iterator.next();
      JsonNode valueNode = entry.getValue();
      if (valueNode == null || valueNode.isNull() || !valueNode.isTextual()) {
        return Optional.empty();
      }
      entries.add(new PartitionEntryImpl(entry.getKey(), valueNode.asText()));
    }

    if (entries.isEmpty()) {
      return Optional.empty();
    }

    return Optional.of(PartitionPath.of(entries));
  }

  /**
   * Parse metric values as numeric statistics only.
   *
   * <p>Non-numeric textual values are skipped and logged.
   */
  private StatisticValue<?> parseStatisticValue(String fieldName, JsonNode node) {
    if (node == null || node.isNull()) {
      return null;
    }

    if (node.isNumber()) {
      return node.isIntegralNumber()
          ? StatisticValues.longValue(node.longValue())
          : StatisticValues.doubleValue(node.doubleValue());
    }

    if (node.isTextual()) {
      String text = node.asText();
      if (StringUtils.isBlank(text)) {
        return null;
      }

      try {
        long longValue = Long.parseLong(text);
        return StatisticValues.longValue(longValue);
      } catch (NumberFormatException e) {
        // Ignore and try parsing as double
      }

      try {
        double doubleValue = Double.parseDouble(text);
        return StatisticValues.doubleValue(doubleValue);
      } catch (NumberFormatException e) {
        LOG.warn("Skip non-numeric textual statistic value for field '{}': {}", fieldName, text);
        return null;
      }
    }

    return null;
  }
}
