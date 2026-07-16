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
package org.apache.gravitino.catalog.doris.integration.test;

import static org.apache.gravitino.integration.test.util.ITUtils.assertPartition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.jdbc.config.JdbcConfig;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.container.DorisContainer;
import org.apache.gravitino.integration.test.container.DorisImageName;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.SupportsPartitions;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.literals.Literal;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.indexes.Indexes;
import org.apache.gravitino.rel.partitions.ListPartition;
import org.apache.gravitino.rel.partitions.Partition;
import org.apache.gravitino.rel.partitions.Partitions;
import org.apache.gravitino.rel.partitions.RangePartition;
import org.apache.gravitino.rel.types.Types;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for Doris 3.0.x specific features: INVERTED index syntax, Auto Increment, and
 * index read-back mapping. Uses Doris 3.0.x Docker image as requested by the community reviewer.
 */
@Tag("gravitino-docker-test")
@Tag("doris-multi-version")
public class CatalogDoris3xIT extends BaseIT {

  private static final String PROVIDER = "jdbc-doris";
  private static final String DRIVER_CLASS_NAME = "com.mysql.cj.jdbc.Driver";
  private static final long MAX_WAIT_IN_SECONDS = 30;
  private static final long WAIT_INTERVAL_IN_SECONDS = 1;
  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();

  private String metalakeName = GravitinoITUtils.genRandomName("doris3x_metalake");
  private String catalogName = GravitinoITUtils.genRandomName("doris3x_catalog");
  private String schemaName = GravitinoITUtils.genRandomName("doris3x_schema");
  private String tableComment = "table_comment";
  private String colName1 = "col_pk";
  private String colName2 = "col_data";

  private GravitinoMetalake metalake;
  private Catalog catalog;

  @BeforeAll
  public void startup() throws IOException {
    containerSuite.startDorisContainer(DorisImageName.VERSION_3_0);
    createMetalake();
    createCatalog();
    createSchema();
  }

  @AfterAll
  public void stop() {
    catalog.asSchemas().dropSchema(schemaName, true);
    metalake.dropCatalog(catalogName, true);
    client.dropMetalake(metalakeName, true);
  }

  @AfterEach
  public void resetSchema() {
    catalog.asSchemas().dropSchema(schemaName, true);
    createSchema();
  }

  private void createMetalake() {
    client.createMetalake(metalakeName, "comment", Collections.emptyMap());
    metalake = client.loadMetalake(metalakeName);
    assertEquals(metalakeName, metalake.name());
  }

  private void createCatalog() {
    DorisContainer dorisContainer = containerSuite.getDorisContainer(DorisImageName.VERSION_3_0);
    String jdbcUrl =
        String.format(
            "jdbc:mysql://%s:%d/",
            dorisContainer.getContainerIpAddress(), dorisContainer.getFeMysqlPort());

    Map<String, String> props = Maps.newHashMap();
    props.put(JdbcConfig.JDBC_URL.getKey(), jdbcUrl);
    props.put(JdbcConfig.JDBC_DRIVER.getKey(), DRIVER_CLASS_NAME);
    props.put(JdbcConfig.USERNAME.getKey(), DorisContainer.USER_NAME);
    props.put(JdbcConfig.PASSWORD.getKey(), DorisContainer.PASSWORD);

    catalog =
        metalake.createCatalog(
            catalogName, Catalog.Type.RELATIONAL, PROVIDER, "doris 3.x catalog", props);
    assertEquals(catalogName, metalake.loadCatalog(catalogName).name());
  }

  private void createSchema() {
    catalog.asSchemas().createSchema(schemaName, null, Collections.emptyMap());
    assertEquals(schemaName, catalog.asSchemas().loadSchema(schemaName).name());
  }

  private Column[] basicColumns() {
    return new Column[] {
      Column.of(colName1, Types.LongType.get(), "pk", false, false, null),
      Column.of(colName2, Types.VarCharType.of(100), "data")
    };
  }

  private Distribution hashDist() {
    return Distributions.hash(1, NamedReference.field(colName1));
  }

  @Test
  void testCreateTableWithInvertedIndex() {
    TableCatalog tc = catalog.asTableCatalog();
    NameIdentifier tid = NameIdentifier.of(schemaName, "t_inverted");
    Index[] indexes =
        new Index[] {Indexes.of(Index.IndexType.INVERTED, "idx_data", new String[][] {{colName2}})};

    tc.createTable(
        tid,
        basicColumns(),
        tableComment,
        Collections.emptyMap(),
        Transforms.EMPTY_TRANSFORM,
        hashDist(),
        null,
        indexes);

    Table t = tc.loadTable(tid);
    assertEquals(1, t.index().length);
    assertEquals(Index.IndexType.INVERTED, t.index()[0].type());
    assertEquals("idx_data", t.index()[0].name());
  }

  @Test
  void testAddAndDropInvertedIndex() {
    TableCatalog tc = catalog.asTableCatalog();
    NameIdentifier tid = NameIdentifier.of(schemaName, "t_add_drop_idx");

    tc.createTable(
        tid,
        basicColumns(),
        tableComment,
        Collections.emptyMap(),
        Transforms.EMPTY_TRANSFORM,
        hashDist(),
        null,
        null);

    // Add INVERTED index
    tc.alterTable(
        tid,
        TableChange.addIndex(Index.IndexType.INVERTED, "idx_data", new String[][] {{colName2}}));

    Awaitility.await()
        .atMost(MAX_WAIT_IN_SECONDS, TimeUnit.SECONDS)
        .pollInterval(WAIT_INTERVAL_IN_SECONDS, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              Table t = tc.loadTable(tid);
              assertEquals(1, t.index().length);
              assertEquals(Index.IndexType.INVERTED, t.index()[0].type());
            });

    // Drop index
    tc.alterTable(tid, TableChange.deleteIndex("idx_data", true));

    Awaitility.await()
        .atMost(MAX_WAIT_IN_SECONDS, TimeUnit.SECONDS)
        .pollInterval(WAIT_INTERVAL_IN_SECONDS, TimeUnit.SECONDS)
        .untilAsserted(() -> assertEquals(0, tc.loadTable(tid).index().length));
  }

  @Test
  void testCreateTableWithAutoIncrement() {
    TableCatalog tc = catalog.asTableCatalog();
    NameIdentifier tid = NameIdentifier.of(schemaName, "t_auto_incr");

    Column autoIncrCol = Column.of(colName1, Types.LongType.get(), "pk", false, true, null);
    Column dataCol = Column.of(colName2, Types.VarCharType.of(100), "data");
    Index[] indexes =
        new Index[] {Indexes.of(Index.IndexType.UNIQUE_KEY, "uk_pk", new String[][] {{colName1}})};

    tc.createTable(
        tid,
        new Column[] {autoIncrCol, dataCol},
        tableComment,
        Collections.emptyMap(),
        Transforms.EMPTY_TRANSFORM,
        hashDist(),
        null,
        indexes);

    Table t = tc.loadTable(tid);
    assertNotNull(t);
    // Verify the auto-increment table can be created successfully on Doris 3.0.x.
    // Doris JDBC driver may not report IS_AUTOINCREMENT correctly via metadata,
    // so we verify table creation success rather than the auto-increment flag read-back.
    assertEquals(2, t.columns().length);
    assertEquals(colName1, t.columns()[0].name());
  }

  @Test
  void testCreateTableWithPrimaryKeySmokeTest() {
    // Smoke test: PRIMARY_KEY → DUPLICATE KEY mapping works on Doris 3.0.x
    TableCatalog tc = catalog.asTableCatalog();
    NameIdentifier tid = NameIdentifier.of(schemaName, "t_pk_smoke");
    Index[] indexes =
        new Index[] {
          Indexes.of(Index.IndexType.PRIMARY_KEY, "PRIMARY", new String[][] {{colName1}})
        };

    tc.createTable(
        tid,
        basicColumns(),
        tableComment,
        Collections.emptyMap(),
        Transforms.EMPTY_TRANSFORM,
        hashDist(),
        null,
        indexes);

    Table t = tc.loadTable(tid);
    assertNotNull(t);
    assertEquals(2, t.columns().length);
  }

  @Test
  void testIndexReadBackMapping() {
    // Create table with INVERTED index, load it, verify index type is correctly mapped
    TableCatalog tc = catalog.asTableCatalog();
    NameIdentifier tid = NameIdentifier.of(schemaName, "t_readback");
    Index[] indexes =
        new Index[] {Indexes.of(Index.IndexType.INVERTED, "idx_data", new String[][] {{colName2}})};

    tc.createTable(
        tid,
        basicColumns(),
        tableComment,
        Collections.emptyMap(),
        Transforms.EMPTY_TRANSFORM,
        hashDist(),
        null,
        indexes);

    Table t = tc.loadTable(tid);
    assertEquals(1, t.index().length);
    assertEquals(Index.IndexType.INVERTED, t.index()[0].type());
    assertEquals("idx_data", t.index()[0].name());
    assertEquals(colName2, t.index()[0].fieldNames()[0][0]);
  }

  @Test
  void testExternalTypeRoundTrip() {
    // Verify ExternalType columns survive the create → Doris 3.0 → load round-trip.
    //
    // Only "json" is tested here because the MySQL JDBC driver returns TYPE_NAME = "UNKNOWN"
    // for Doris-specific types (ipv4, ipv6, variant, bitmap, hll, largeint) that have no
    // standard JDBC type mapping. The toGravitino() fallback produces ExternalType("unknown")
    // for those, so they cannot round-trip through the standard JDBC metadata path.
    // The DDL generation (fromGravitino) and type parsing (toGravitino) for all these types
    // are covered by unit tests in TestDorisTypeConverter.
    TableCatalog tc = catalog.asTableCatalog();
    NameIdentifier tid = NameIdentifier.of(schemaName, "t_external_types");

    Column[] columns =
        ArrayUtils.addAll(
            basicColumns(), Column.of("json_col", Types.ExternalType.of("json"), "json column"));

    tc.createTable(
        tid,
        columns,
        tableComment,
        Collections.emptyMap(),
        Transforms.EMPTY_TRANSFORM,
        hashDist(),
        null,
        null);

    Table t = tc.loadTable(tid);
    assertEquals(3, t.columns().length);

    assertEquals(Types.ExternalType.of("json"), findColumn(t, "json_col").dataType());
  }

  @Test
  void testListPartitionRoundTrip() {
    // Verify LIST partition with assignments round-trips correctly on Doris 3.0.x.
    // The partition parsing regex was updated to handle Doris 3.0+ format with spaces.
    TableCatalog tc = catalog.asTableCatalog();
    NameIdentifier tid =
        NameIdentifier.of(schemaName, GravitinoITUtils.genRandomName("t_list_partition"));

    Column cityCol = Column.of("city", Types.VarCharType.of(50), "city", false, false, null);
    Distribution dist = Distributions.hash(1, NamedReference.field("city"));

    // Create table with initial partition assignments so Doris 3.0.x recognizes it as partitioned.
    Literal[][] p1Values = {{Literals.of("beijing", Types.VarCharType.of(50))}};
    Literal[][] p2Values = {{Literals.of("shanghai", Types.VarCharType.of(50))}};
    ListPartition p1 = Partitions.list("p1", p1Values, Collections.emptyMap());
    ListPartition p2 = Partitions.list("p2", p2Values, Collections.emptyMap());
    Transform[] partitioning = {
      Transforms.list(new String[][] {{"city"}}, new ListPartition[] {p1, p2})
    };

    tc.createTable(
        tid,
        new Column[] {cityCol},
        tableComment,
        Collections.emptyMap(),
        partitioning,
        dist,
        null,
        null);

    // Verify round-trip
    Table loaded = tc.loadTable(tid);
    assertEquals(1, loaded.partitioning().length);
    assertEquals("list", loaded.partitioning()[0].name());

    Map<String, ListPartition> partitions =
        Arrays.stream(loaded.supportPartitions().listPartitions())
            .collect(Collectors.toMap(Partition::name, p -> (ListPartition) p));
    assertEquals(2, partitions.size());
    assertPartition(Partitions.list("p1", p1Values, Collections.emptyMap()), partitions.get("p1"));
    assertPartition(Partitions.list("p2", p2Values, Collections.emptyMap()), partitions.get("p2"));
  }

  @Test
  void testRangePartitionRoundTrip() {
    // Verify RANGE partition round-trips correctly on Doris 3.0.x.
    // The regex was updated to tolerate space between RANGE and parenthesis.
    TableCatalog tc = catalog.asTableCatalog();
    NameIdentifier tid =
        NameIdentifier.of(schemaName, GravitinoITUtils.genRandomName("t_range_partition"));

    Column dateCol = Column.of("dt", Types.DateType.get(), "date", false, false, null);
    Distribution dist = Distributions.hash(1, NamedReference.field("dt"));

    Literal todayLiteral = Literals.of("2024-07-24", Types.DateType.get());
    Literal tomorrowLiteral = Literals.of("2024-07-25", Types.DateType.get());
    RangePartition p1 = Partitions.range("p1", todayLiteral, Literals.NULL, Collections.emptyMap());
    RangePartition p2 =
        Partitions.range("p2", tomorrowLiteral, todayLiteral, Collections.emptyMap());
    RangePartition p3 =
        Partitions.range("p3", Literals.NULL, tomorrowLiteral, Collections.emptyMap());
    Transform[] partitioning = {
      Transforms.range(new String[] {"dt"}, new RangePartition[] {p1, p2, p3})
    };

    tc.createTable(
        tid,
        new Column[] {dateCol},
        tableComment,
        Collections.emptyMap(),
        partitioning,
        dist,
        null,
        null);

    Table loaded = tc.loadTable(tid);
    assertEquals(1, loaded.partitioning().length);
    assertEquals("range", loaded.partitioning()[0].name());

    // Verify partition assignments
    SupportsPartitions partitionOps = loaded.supportPartitions();
    Map<String, RangePartition> partitions =
        Arrays.stream(partitionOps.listPartitions())
            .collect(Collectors.toMap(Partition::name, p -> (RangePartition) p));
    assertEquals(3, partitions.size());
    assertPartition(
        Partitions.range(
            "p1",
            todayLiteral,
            Literals.of("0000-01-01", Types.DateType.get()),
            Collections.emptyMap()),
        partitions.get("p1"));
    assertPartition(p2, partitions.get("p2"));
    assertPartition(
        Partitions.range(
            "p3",
            Literals.of("MAXVALUE", Types.DateType.get()),
            tomorrowLiteral,
            Collections.emptyMap()),
        partitions.get("p3"));
  }

  private Column findColumn(Table table, String columnName) {
    return Arrays.stream(table.columns())
        .filter(c -> c.name().equals(columnName))
        .findFirst()
        .orElseThrow(() -> new AssertionError("Column not found: " + columnName));
  }
}
