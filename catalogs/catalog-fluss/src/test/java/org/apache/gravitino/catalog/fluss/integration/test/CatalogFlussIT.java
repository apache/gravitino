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

package org.apache.gravitino.catalog.fluss.integration.test;

import static org.apache.gravitino.catalog.fluss.integration.test.FlussCluster.BOOTSTRAP_SERVERS;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Map;
import org.apache.fluss.config.ConfigOptions;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.Schema;
import org.apache.gravitino.catalog.fluss.FlussCatalogOperations;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.NonEmptySchemaException;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.SupportsPartitions;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.literals.Literal;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.expressions.sorts.SortOrders;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.indexes.Indexes;
import org.apache.gravitino.rel.partitions.IdentityPartition;
import org.apache.gravitino.rel.partitions.Partition;
import org.apache.gravitino.rel.partitions.Partitions;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.rest.RESTUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/** Integration tests for Fluss catalog operations against a minimal Fluss Docker cluster. */
@Tag("gravitino-docker-test")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class CatalogFlussIT {

  private static final Namespace CATALOG_NAMESPACE = Namespace.of("metalake", "catalog");

  private FlussCluster cluster;
  private FlussCatalogOperations ops;
  private String currentSchema;

  @BeforeAll
  void startCluster() throws Exception {
    cluster = FlussCluster.start();
    ops = new FlussCatalogOperations();
    ops.initialize(catalogConfig(), null, null);
  }

  @AfterEach
  void cleanupSchema() {
    if (currentSchema == null) {
      return;
    }

    try {
      ops.dropSchema(schemaIdent(currentSchema), true);
    } catch (Exception ignored) {
    } finally {
      currentSchema = null;
    }
  }

  @AfterAll
  void stopCluster() throws Exception {
    if (ops != null) {
      ops.close();
    }
    if (cluster != null) {
      cluster.close();
    }
  }

  @Test
  void testConnection() throws Exception {
    assertDoesNotThrow(
        () ->
            ops.testConnection(
                NameIdentifier.of("metalake", "catalog"),
                Catalog.Type.RELATIONAL,
                "fluss",
                null,
                catalogConfig()));

    String unusedPort = String.valueOf(RESTUtils.findAvailablePort(0, 0));
    try (FlussCatalogOperations invalidOps = new FlussCatalogOperations()) {
      assertThrows(
          RuntimeException.class,
          () ->
              invalidOps.initialize(
                  Map.of(
                      BOOTSTRAP_SERVERS,
                      "localhost:" + unusedPort,
                      BaseCatalog.CATALOG_BYPASS_PREFIX
                          + ConfigOptions.CLIENT_REQUEST_TIMEOUT.key(),
                      "2 s"),
                  null,
                  null));
    }
  }

  @Test
  void testSchemaCrud() {
    String schema = newSchema();

    assertFalse(ops.schemaExists(schemaIdent(schema)));
    Schema created = ops.createSchema(schemaIdent(schema), "schema comment", Map.of("k1", "v1"));

    assertEquals(schema, created.name());
    assertEquals("schema comment", created.comment());
    assertEquals("v1", created.properties().get("k1"));
    assertTrue(ops.schemaExists(schemaIdent(schema)));
    assertTrue(
        Arrays.stream(ops.listSchemas(CATALOG_NAMESPACE))
            .map(NameIdentifier::name)
            .anyMatch(schema::equals));

    Schema loaded = ops.loadSchema(schemaIdent(schema));
    assertEquals(created.name(), loaded.name());
    assertEquals(created.comment(), loaded.comment());

    assertTrue(ops.dropSchema(schemaIdent(schema), false));
    currentSchema = null;
    assertFalse(ops.schemaExists(schemaIdent(schema)));
  }

  @Test
  void testDropNonEmptySchema() {
    String schema = newSchema();
    ops.createSchema(schemaIdent(schema), null, Map.of());
    ops.createTable(
        tableIdent(schema, "orders"),
        basicColumns(),
        null,
        Map.of(),
        new Transform[0],
        Distributions.NONE,
        SortOrders.NONE,
        Indexes.EMPTY_INDEXES);

    assertThrows(NonEmptySchemaException.class, () -> ops.dropSchema(schemaIdent(schema), false));
    assertTrue(ops.dropSchema(schemaIdent(schema), true));
    currentSchema = null;
  }

  @Test
  void testTableCrud() {
    String schema = newSchema();
    NameIdentifier tableIdent = tableIdent(schema, "orders");
    ops.createSchema(schemaIdent(schema), null, Map.of());

    Table created =
        ops.createTable(
            tableIdent,
            basicColumns(),
            "orders comment",
            Map.of("table.log.ttl", "1d"),
            new Transform[0],
            Distributions.hash(3, NamedReference.field("site_id")),
            SortOrders.NONE,
            Indexes.EMPTY_INDEXES);

    assertEquals("orders", created.name());
    assertEquals("orders comment", created.comment());
    assertEquals("1d", created.properties().get("table.log.ttl"));
    assertEquals(3, created.distribution().number());
    assertEquals(3, created.columns().length);

    assertTrue(
        Arrays.stream(ops.listTables(Namespace.of("metalake", "catalog", schema)))
            .map(NameIdentifier::name)
            .anyMatch("orders"::equals));
    assertEquals("orders", ops.loadTable(tableIdent).name());
    assertTrue(ops.dropTable(tableIdent));
    assertThrows(NoSuchTableException.class, () -> ops.loadTable(tableIdent));
    assertFalse(ops.dropTable(tableIdent));
  }

  @Test
  void testAlterTable() {
    String schema = newSchema();
    NameIdentifier tableIdent = tableIdent(schema, "orders");
    ops.createSchema(schemaIdent(schema), null, Map.of());
    ops.createTable(
        tableIdent,
        basicColumns(),
        null,
        Map.of(),
        new Transform[0],
        Distributions.NONE,
        SortOrders.NONE,
        Indexes.EMPTY_INDEXES);

    Table withColumn =
        ops.alterTable(
            tableIdent, TableChange.addColumn(new String[] {"country"}, Types.StringType.get()));
    assertTrue(
        Arrays.stream(withColumn.columns()).anyMatch(column -> column.name().equals("country")));
  }

  @Test
  void testPartitionCrud() {
    String schema = newSchema();
    NameIdentifier tableIdent = tableIdent(schema, "partitioned_orders");
    ops.createSchema(schemaIdent(schema), null, Map.of());
    ops.createTable(
        tableIdent,
        partitionedColumns(),
        null,
        Map.of(),
        new Transform[] {Transforms.identity("event_day")},
        Distributions.NONE,
        SortOrders.NONE,
        Indexes.EMPTY_INDEXES);

    SupportsPartitions partitions = ops.loadTable(tableIdent).supportPartitions();
    IdentityPartition partition = identityPartition("2026-05-26");

    Partition added = partitions.addPartition(partition);
    assertEquals(partition.name(), added.name());
    assertArrayEquals(new String[] {partition.name()}, partitions.listPartitionNames());
    assertNotNull(partitions.getPartition(partition.name()));

    assertTrue(partitions.dropPartition(partition.name()));
    assertFalse(partitions.dropPartition(partition.name()));
  }

  private Map<String, String> catalogConfig() {
    return Map.of(BOOTSTRAP_SERVERS, cluster.bootstrapServers());
  }

  private String newSchema() {
    currentSchema = "fluss_it_" + System.nanoTime();
    return currentSchema;
  }

  private NameIdentifier schemaIdent(String schema) {
    return NameIdentifier.of("metalake", "catalog", schema);
  }

  private NameIdentifier tableIdent(String schema, String table) {
    return NameIdentifier.of("metalake", "catalog", schema, table);
  }

  private Column[] basicColumns() {
    return new Column[] {
      Column.of("event_day", Types.StringType.get(), "event day", false, false, null),
      Column.of("site_id", Types.IntegerType.get(), "site", false, false, null),
      Column.of("pv", Types.LongType.get(), "page views", true, false, null)
    };
  }

  private Column[] partitionedColumns() {
    return new Column[] {
      Column.of("event_day", Types.StringType.get(), "event day", false, false, null),
      Column.of("site_id", Types.IntegerType.get(), "site", false, false, null)
    };
  }

  private IdentityPartition identityPartition(String dateValue) {
    return Partitions.identity(
        "event_day=" + dateValue,
        new String[][] {{"event_day"}},
        new Literal[] {Literals.stringLiteral(dateValue)},
        Map.of());
  }
}
