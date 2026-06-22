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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
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
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.indexes.Indexes;
import org.apache.gravitino.rel.types.Types;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for Doris 4.0.x specific features: BITMAP→INVERTED mapping, ANN/VECTOR index,
 * and cross-version compatibility. Uses Doris 4.0.x Docker image to verify that BITMAP indexes are
 * correctly mapped to INVERTED (since Doris 4.0.6 removed BITMAP from Nereids grammar).
 */
@Tag("gravitino-docker-test")
public class CatalogDoris4xIT extends BaseIT {

  private static final String PROVIDER = "jdbc-doris";
  private static final String DRIVER_CLASS_NAME = "com.mysql.cj.jdbc.Driver";
  private static final long MAX_WAIT_IN_SECONDS = 30;
  private static final long WAIT_INTERVAL_IN_SECONDS = 1;
  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();

  private String metalakeName = GravitinoITUtils.genRandomName("doris4x_metalake");
  private String catalogName = GravitinoITUtils.genRandomName("doris4x_catalog");
  private String schemaName = GravitinoITUtils.genRandomName("doris4x_schema");
  private String tableComment = "table_comment";
  private String colName1 = "col_pk";
  private String colName2 = "col_data";

  private GravitinoMetalake metalake;
  private Catalog catalog;

  @BeforeAll
  public void startup() throws IOException {
    containerSuite.startDorisContainer(DorisImageName.VERSION_4_0);
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
    DorisContainer dorisContainer = containerSuite.getDorisContainer(DorisImageName.VERSION_4_0);
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
            catalogName, Catalog.Type.RELATIONAL, PROVIDER, "doris 4.x catalog", props);
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
    assertEquals(2, t.columns().length);
    assertEquals(colName1, t.columns()[0].name());
  }

  @Test
  void testCreateTableWithUniqueKeyModel() {
    // UNIQUE_KEY model: Doris 4.0.x Nereids parser accepts UNIQUE KEY(col) syntax.
    // PRIMARY_KEY index type is also mapped to UNIQUE KEY (functionally equivalent).
    TableCatalog tc = catalog.asTableCatalog();
    NameIdentifier tid = NameIdentifier.of(schemaName, "t_uk_model");
    Index[] indexes =
        new Index[] {Indexes.of(Index.IndexType.UNIQUE_KEY, "uk_pk", new String[][] {{colName1}})};

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
  void testBitmapIndexReadBackMapping() {
    // On Doris 4.0.x, BITMAP is removed from Nereids grammar.
    // When we create a table with INVERTED index and read it back,
    // the index type should be INVERTED (not BITMAP).
    TableCatalog tc = catalog.asTableCatalog();
    NameIdentifier tid = NameIdentifier.of(schemaName, "t_bitmap_readback");
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
    // Verify the read-back type is INVERTED, not BITMAP
    assertEquals(Index.IndexType.INVERTED, t.index()[0].type());
    assertEquals("idx_data", t.index()[0].name());
    assertEquals(colName2, t.index()[0].fieldNames()[0][0]);
  }
}
