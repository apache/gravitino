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
package org.apache.gravitino.client.integration.test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.exceptions.IllegalStatisticNameException;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.container.HiveContainer;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.distributions.Strategy;
import org.apache.gravitino.rel.expressions.sorts.NullOrdering;
import org.apache.gravitino.rel.expressions.sorts.SortDirection;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.sorts.SortOrders;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.stats.PartitionRange;
import org.apache.gravitino.stats.PartitionStatistics;
import org.apache.gravitino.stats.PartitionStatisticsDrop;
import org.apache.gravitino.stats.PartitionStatisticsModification;
import org.apache.gravitino.stats.PartitionStatisticsUpdate;
import org.apache.gravitino.stats.Statistic;
import org.apache.gravitino.stats.StatisticValue;
import org.apache.gravitino.stats.StatisticValues;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("gravitino-docker-test")
public class StatisticIT extends BaseIT {

  private static final Logger LOG = LoggerFactory.getLogger(StatisticIT.class);
  public static final String metalakeName =
      GravitinoITUtils.genRandomName("cataloghiveit_metalake");
  public String catalogName = GravitinoITUtils.genRandomName("cataloghiveit_catalog");
  public String SCHEMA_PREFIX = "cataloghiveit_schema";
  public String schemaName = GravitinoITUtils.genRandomName(SCHEMA_PREFIX);
  public String TABLE_PREFIX = "cataloghiveit_table";
  public String tableName = GravitinoITUtils.genRandomName(TABLE_PREFIX);
  public static final String TABLE_COMMENT = "table_comment";
  public static final String HIVE_COL_NAME1 = "hive_col_name1";
  public static final String HIVE_COL_NAME2 = "hive_col_name2";
  public static final String HIVE_COL_NAME3 = "hive_col_name3";
  private String HIVE_METASTORE_URIS;
  private final String provider = "hive";
  private final ContainerSuite containerSuite = ContainerSuite.getInstance();
  private GravitinoMetalake metalake;
  private Catalog catalog;
  private Table table;

  @BeforeAll
  public void startup() {
    startNecessaryContainer();

    createMetalake();
    createCatalog();
    createSchema();
    createTable();
  }

  @Test
  public void testTableStatisticLifeCycle() {
    // list empty statistics
    List<Statistic> statistics = table.supportsStatistics().listStatistics();
    Assertions.assertTrue(statistics.isEmpty());

    // update statistics
    Map<String, StatisticValue<?>> updateStatistics = Maps.newHashMap();
    updateStatistics.put("custom-k1", StatisticValues.stringValue("v1"));
    updateStatistics.put("custom-k2", StatisticValues.stringValue("v2"));
    table.supportsStatistics().updateStatistics(updateStatistics);

    statistics = table.supportsStatistics().listStatistics();
    Assertions.assertEquals(2, statistics.size());
    statistics.sort(Comparator.comparing(Statistic::name));

    Assertions.assertEquals(statistics.get(0).name(), "custom-k1");
    Assertions.assertEquals(statistics.get(0).value().get(), StatisticValues.stringValue("v1"));
    Assertions.assertEquals(statistics.get(1).name(), "custom-k2");
    Assertions.assertEquals(statistics.get(1).value().get(), StatisticValues.stringValue("v2"));

    // update partial statistics
    updateStatistics.clear();
    updateStatistics.put("custom-k1", StatisticValues.stringValue("v1-1"));
    updateStatistics.put("custom-k3", StatisticValues.stringValue("v3"));
    table.supportsStatistics().updateStatistics(updateStatistics);

    statistics = table.supportsStatistics().listStatistics();
    Assertions.assertEquals(3, statistics.size());
    statistics.sort(Comparator.comparing(Statistic::name));

    Assertions.assertEquals(statistics.get(0).name(), "custom-k1");
    Assertions.assertEquals(statistics.get(0).value().get(), StatisticValues.stringValue("v1-1"));
    Assertions.assertEquals(statistics.get(1).name(), "custom-k2");
    Assertions.assertEquals(statistics.get(1).value().get(), StatisticValues.stringValue("v2"));
    Assertions.assertEquals(statistics.get(2).name(), "custom-k3");
    Assertions.assertEquals(statistics.get(2).value().get(), StatisticValues.stringValue("v3"));

    // update illegal stats
    Map<String, StatisticValue<?>> illegalUpdateStatistics = Maps.newHashMap();
    illegalUpdateStatistics.put("k1", StatisticValues.stringValue("v1-2"));
    Assertions.assertThrows(
        IllegalStatisticNameException.class,
        () -> table.supportsStatistics().updateStatistics(illegalUpdateStatistics));

    // drop statistics
    List<String> statisticsToDrop = Lists.newArrayList("custom-k1");
    table.supportsStatistics().dropStatistics(statisticsToDrop);

    statistics = table.supportsStatistics().listStatistics();
    Assertions.assertEquals(2, statistics.size());

    statistics.sort(Comparator.comparing(Statistic::name));
    Assertions.assertEquals(statistics.get(0).name(), "custom-k2");
    Assertions.assertEquals(statistics.get(0).value().get(), StatisticValues.stringValue("v2"));
    Assertions.assertEquals(statistics.get(1).name(), "custom-k3");
    Assertions.assertEquals(statistics.get(1).value().get(), StatisticValues.stringValue("v3"));
  }

  @Test
  public void testPartitionStatisticLifeCycle() {
    // list empty partition statistics
    PartitionRange range = PartitionRange.downTo("p0", PartitionRange.BoundType.CLOSED);
    table.supportsPartitionStatistics().listPartitionStatistics(range);

    // update partition statistics
    List<PartitionStatisticsUpdate> statisticsToUpdate = Lists.newArrayList();
    Map<String, StatisticValue<?>> stats = Maps.newHashMap();
    stats.put("custom-k1", StatisticValues.stringValue("v1"));
    stats.put("custom-k2", StatisticValues.stringValue("v2"));
    statisticsToUpdate.add(PartitionStatisticsModification.update("p1", stats));
    table.supportsPartitionStatistics().updatePartitionStatistics(statisticsToUpdate);

    List<PartitionStatistics> listedStats =
        table.supportsPartitionStatistics().listPartitionStatistics(range);
    Assertions.assertEquals(1, listedStats.size());
    PartitionStatistics partitionStatistics = listedStats.get(0);
    Assertions.assertEquals("p1", partitionStatistics.partitionName());
    Statistic[] statistics = partitionStatistics.statistics();
    Assertions.assertEquals(2, statistics.length);
    Arrays.sort(statistics, Comparator.comparing(Statistic::name));
    Assertions.assertEquals(statistics[0].name(), "custom-k1");
    Assertions.assertEquals(statistics[0].value().get(), StatisticValues.stringValue("v1"));
    Assertions.assertEquals(statistics[1].name(), "custom-k2");
    Assertions.assertEquals(statistics[1].value().get(), StatisticValues.stringValue("v2"));

    // update partial partition statistics
    statisticsToUpdate.clear();
    stats.clear();
    stats.put("custom-k1", StatisticValues.stringValue("v1-1"));
    stats.put("custom-k3", StatisticValues.stringValue("v3"));
    statisticsToUpdate.add(PartitionStatisticsModification.update("p0", stats));
    statisticsToUpdate.add(PartitionStatisticsModification.update("p1", stats));
    table.supportsPartitionStatistics().updatePartitionStatistics(statisticsToUpdate);

    listedStats = table.supportsPartitionStatistics().listPartitionStatistics(range);
    Assertions.assertEquals(2, listedStats.size());
    listedStats.sort(Comparator.comparing(PartitionStatistics::partitionName));
    partitionStatistics = listedStats.get(0);
    Assertions.assertEquals("p0", partitionStatistics.partitionName());
    statistics = partitionStatistics.statistics();
    Assertions.assertEquals(2, statistics.length);
    Arrays.sort(statistics, Comparator.comparing(Statistic::name));
    Assertions.assertEquals(statistics[0].name(), "custom-k1");
    Assertions.assertEquals(statistics[0].value().get(), StatisticValues.stringValue("v1-1"));
    Assertions.assertEquals(statistics[1].name(), "custom-k3");
    Assertions.assertEquals(statistics[1].value().get(), StatisticValues.stringValue("v3"));
    partitionStatistics = listedStats.get(1);
    Assertions.assertEquals("p1", partitionStatistics.partitionName());
    statistics = partitionStatistics.statistics();
    Assertions.assertEquals(3, statistics.length);
    Arrays.sort(statistics, Comparator.comparing(Statistic::name));
    Assertions.assertEquals(statistics[0].name(), "custom-k1");
    Assertions.assertEquals(statistics[0].value().get(), StatisticValues.stringValue("v1-1"));
    Assertions.assertEquals(statistics[1].name(), "custom-k2");
    Assertions.assertEquals(statistics[1].value().get(), StatisticValues.stringValue("v2"));
    Assertions.assertEquals(statistics[2].name(), "custom-k3");
    Assertions.assertEquals(statistics[2].value().get(), StatisticValues.stringValue("v3"));

    // update illegal partition stats
    Map<String, StatisticValue<?>> illegalUpdateStatistics = Maps.newHashMap();
    illegalUpdateStatistics.put("k1", StatisticValues.stringValue("v1-2"));
    Assertions.assertThrows(
        IllegalStatisticNameException.class,
        () ->
            table
                .supportsPartitionStatistics()
                .updatePartitionStatistics(
                    Lists.newArrayList(
                        PartitionStatisticsModification.update("p0", illegalUpdateStatistics))));

    // drop partition statistics
    List<PartitionStatisticsDrop> statisticsToDrop = Lists.newArrayList();
    statisticsToDrop.add(
        PartitionStatisticsModification.drop("p0", Lists.newArrayList("custom-k1")));

    table.supportsPartitionStatistics().dropPartitionStatistics(statisticsToDrop);

    listedStats = table.supportsPartitionStatistics().listPartitionStatistics(range);
    Assertions.assertEquals(2, listedStats.size());
    listedStats.sort(Comparator.comparing(PartitionStatistics::partitionName));
    partitionStatistics = listedStats.get(0);
    Assertions.assertEquals("p0", partitionStatistics.partitionName());
    statistics = partitionStatistics.statistics();
    Assertions.assertEquals(1, statistics.length);
    Assertions.assertEquals(statistics[0].name(), "custom-k3");
    Assertions.assertEquals(statistics[0].value().get(), StatisticValues.stringValue("v3"));

    partitionStatistics = listedStats.get(1);
    Assertions.assertEquals("p1", partitionStatistics.partitionName());
    statistics = partitionStatistics.statistics();
    Assertions.assertEquals(3, statistics.length);
    Arrays.sort(statistics, Comparator.comparing(Statistic::name));
    Assertions.assertEquals(statistics[0].name(), "custom-k1");
    Assertions.assertEquals(statistics[0].value().get(), StatisticValues.stringValue("v1-1"));
    Assertions.assertEquals(statistics[1].name(), "custom-k2");
    Assertions.assertEquals(statistics[1].value().get(), StatisticValues.stringValue("v2"));
    Assertions.assertEquals(statistics[2].name(), "custom-k3");
    Assertions.assertEquals(statistics[2].value().get(), StatisticValues.stringValue("v3"));

    // Test range cases
    range = PartitionRange.downTo("p1", PartitionRange.BoundType.OPEN);
    listedStats = table.supportsPartitionStatistics().listPartitionStatistics(range);
    Assertions.assertTrue(listedStats.isEmpty());

    range = PartitionRange.upTo("p1", PartitionRange.BoundType.OPEN);
    listedStats = table.supportsPartitionStatistics().listPartitionStatistics(range);
    Assertions.assertEquals(1, listedStats.size());

    range = PartitionRange.upTo("p1", PartitionRange.BoundType.CLOSED);
    listedStats = table.supportsPartitionStatistics().listPartitionStatistics(range);
    Assertions.assertEquals(2, listedStats.size());
  }

  protected void startNecessaryContainer() {
    containerSuite.startHiveContainer();

    HIVE_METASTORE_URIS =
        String.format(
            "thrift://%s:%d",
            containerSuite.getHiveContainer().getContainerIpAddress(),
            HiveContainer.HIVE_METASTORE_PORT);
  }

  private void createMetalake() {
    GravitinoMetalake[] gravitinoMetalakes = client.listMetalakes();
    Assertions.assertEquals(0, gravitinoMetalakes.length);

    client.createMetalake(metalakeName, "comment", Collections.emptyMap());
    GravitinoMetalake loadMetalake = client.loadMetalake(metalakeName);
    Assertions.assertEquals(metalakeName, loadMetalake.name());

    metalake = loadMetalake;
  }

  protected void createCatalog() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put("metastore.uris", HIVE_METASTORE_URIS);

    metalake.createCatalog(catalogName, Catalog.Type.RELATIONAL, provider, "comment", properties);

    catalog = metalake.loadCatalog(catalogName);
  }

  private void createSchema() {
    Map<String, String> schemaProperties = createSchemaProperties();
    String comment = "comment";
    catalog.asSchemas().createSchema(schemaName, comment, schemaProperties);
  }

  private void createTable() {
    Column[] columns = createColumns();

    NameIdentifier nameIdentifier = NameIdentifier.of(schemaName, tableName);

    Distribution distribution =
        Distributions.of(Strategy.EVEN, 10, NamedReference.field(HIVE_COL_NAME1));

    final SortOrder[] sortOrders =
        new SortOrder[] {
          SortOrders.of(
              NamedReference.field(HIVE_COL_NAME2),
              SortDirection.DESCENDING,
              NullOrdering.NULLS_FIRST)
        };

    Map<String, String> properties = createProperties();
    table =
        catalog
            .asTableCatalog()
            .createTable(
                nameIdentifier,
                columns,
                TABLE_COMMENT,
                properties,
                Transforms.EMPTY_TRANSFORM,
                distribution,
                sortOrders);
  }

  @AfterAll
  public void stop() {
    if (client != null) {
      Arrays.stream(catalog.asSchemas().listSchemas())
          .filter(schema -> !schema.equals("default"))
          .forEach(
              (schema -> {
                catalog.asSchemas().dropSchema(schema, true);
              }));
      Arrays.stream(metalake.listCatalogs())
          .forEach(
              catalogName -> {
                metalake.dropCatalog(catalogName, true);
              });
      client.dropMetalake(metalakeName, true);
    }
    try {
      closer.close();
    } catch (Exception e) {
      LOG.error("Failed to close CloseableGroup", e);
    }

    client = null;
  }

  protected Map<String, String> createSchemaProperties() {
    Map<String, String> properties = new HashMap<>();
    properties.put("key1", "val1");
    properties.put("key2", "val2");
    properties.put(
        "location",
        String.format(
            "hdfs://%s:%d/user/hive/warehouse/%s.db",
            containerSuite.getHiveContainer().getContainerIpAddress(),
            HiveContainer.HDFS_DEFAULTFS_PORT,
            schemaName.toLowerCase()));
    return properties;
  }

  private Column[] createColumns() {
    Column col1 = Column.of(HIVE_COL_NAME1, Types.ByteType.get(), "col_1_comment");
    Column col2 = Column.of(HIVE_COL_NAME2, Types.DateType.get(), "col_2_comment");
    Column col3 = Column.of(HIVE_COL_NAME3, Types.StringType.get(), "col_3_comment");
    return new Column[] {col1, col2, col3};
  }

  protected Map<String, String> createProperties() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put("key1", "val1");
    properties.put("key2", "val2");
    return properties;
  }
}
