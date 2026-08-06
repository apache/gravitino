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

import static org.apache.gravitino.connector.BaseCatalog.CATALOG_BYPASS_PREFIX;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.fluss.config.ConfigOptions;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.Schema;
import org.apache.gravitino.SupportsSchemas;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.exceptions.ConnectionFailedException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.NonEmptySchemaException;
import org.apache.gravitino.integration.test.container.FlussContainer;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.SupportsPartitions;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
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
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/** Integration tests for Fluss catalog operations through Gravitino server APIs. */
@Tag("gravitino-docker-test")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class CatalogFlussIT extends BaseIT {

  private static final String PROVIDER = "fluss";
  private static final String METALAKE_NAME = GravitinoITUtils.genRandomName("fluss_it_metalake");
  private static final String CATALOG_NAME = GravitinoITUtils.genRandomName("fluss_it_catalog");

  private GravitinoMetalake metalake;
  private Catalog catalog;
  private String currentSchema;

  @BeforeAll
  void startUp() {
    containerSuite.startFlussContainer();
    createMetalake();

    Map<String, String> properties = catalogProperties(containerSuite.getFlussContainer());
    waitUntilConnectionSucceeds(CATALOG_NAME, properties);
    catalog = createCatalog(CATALOG_NAME, "Fluss catalog for IT", properties);
  }

  @AfterEach
  void cleanupSchema() {
    if (currentSchema == null || catalog == null) {
      return;
    }

    try {
      catalog.asSchemas().dropSchema(currentSchema, true);
    } catch (Exception ignored) {
    } finally {
      currentSchema = null;
    }
  }

  @AfterAll
  void stop() {
    if (metalake != null) {
      try {
        metalake.dropCatalog(CATALOG_NAME, true);
      } catch (Exception ignored) {
      }
    }
    if (client != null) {
      try {
        client.dropMetalake(METALAKE_NAME, true);
      } catch (Exception ignored) {
      }
    }
  }

  @Test
  void testConnection() throws Exception {
    assertDoesNotThrow(
        () ->
            metalake.testConnection(
                GravitinoITUtils.genRandomName("test_fluss_catalog"),
                Catalog.Type.RELATIONAL,
                PROVIDER,
                "Fluss catalog for IT",
                catalogProperties(containerSuite.getFlussContainer())));

    String unusedPort = String.valueOf(RESTUtils.findAvailablePort(0, 0));
    assertThrows(
        ConnectionFailedException.class,
        () ->
            metalake.testConnection(
                GravitinoITUtils.genRandomName("invalid_fluss_catalog"),
                Catalog.Type.RELATIONAL,
                PROVIDER,
                "Invalid Fluss catalog",
                invalidCatalogProperties(unusedPort)));
  }

  @Test
  void testSchemaCrud() {
    SupportsSchemas schemas = catalog.asSchemas();
    String schema = newSchema();

    assertFalse(schemas.schemaExists(schema));
    Schema created = schemas.createSchema(schema, "schema comment", Map.of("k1", "v1"));

    assertEquals(schema, created.name());
    assertEquals("schema comment", created.comment());
    assertEquals("v1", created.properties().get("k1"));
    assertTrue(schemas.schemaExists(schema));
    assertTrue(Arrays.asList(schemas.listSchemas()).contains(schema));

    Schema loaded = schemas.loadSchema(schema);
    assertEquals(created.name(), loaded.name());
    assertEquals(created.comment(), loaded.comment());

    assertTrue(schemas.dropSchema(schema, false));
    currentSchema = null;
    assertFalse(schemas.schemaExists(schema));
    assertThrows(NoSuchSchemaException.class, () -> schemas.loadSchema(schema));
  }

  @Test
  void testDropNonEmptySchema() {
    SupportsSchemas schemas = catalog.asSchemas();
    TableCatalog tableCatalog = catalog.asTableCatalog();
    String schema = newSchema();
    schemas.createSchema(schema, null, Map.of());
    tableCatalog.createTable(
        tableIdent(schema, "orders"),
        basicColumns(),
        null,
        Map.of(),
        new Transform[0],
        Distributions.NONE,
        SortOrders.NONE,
        Indexes.EMPTY_INDEXES);

    assertThrows(NonEmptySchemaException.class, () -> schemas.dropSchema(schema, false));
    assertTrue(schemas.dropSchema(schema, true));
    currentSchema = null;
  }

  @Test
  void testTableCrud() {
    SupportsSchemas schemas = catalog.asSchemas();
    TableCatalog tableCatalog = catalog.asTableCatalog();
    String schema = newSchema();
    NameIdentifier tableIdent = tableIdent(schema, "orders");
    schemas.createSchema(schema, null, Map.of());

    Table created =
        tableCatalog.createTable(
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
        Arrays.stream(tableCatalog.listTables(Namespace.of(schema)))
            .map(NameIdentifier::name)
            .anyMatch("orders"::equals));
    assertEquals("orders", tableCatalog.loadTable(tableIdent).name());
    assertTrue(tableCatalog.dropTable(tableIdent));
    assertThrows(NoSuchTableException.class, () -> tableCatalog.loadTable(tableIdent));
    assertFalse(tableCatalog.dropTable(tableIdent));
  }

  @Test
  void testAlterTable() {
    SupportsSchemas schemas = catalog.asSchemas();
    TableCatalog tableCatalog = catalog.asTableCatalog();
    String schema = newSchema();
    NameIdentifier tableIdent = tableIdent(schema, "orders");
    schemas.createSchema(schema, null, Map.of());
    tableCatalog.createTable(
        tableIdent,
        basicColumns(),
        null,
        Map.of(),
        new Transform[0],
        Distributions.NONE,
        SortOrders.NONE,
        Indexes.EMPTY_INDEXES);

    Table withColumn =
        tableCatalog.alterTable(
            tableIdent, TableChange.addColumn(new String[] {"country"}, Types.StringType.get()));
    assertTrue(
        Arrays.stream(withColumn.columns()).anyMatch(column -> column.name().equals("country")));
  }

  @Test
  void testPartitionCrud() {
    SupportsSchemas schemas = catalog.asSchemas();
    TableCatalog tableCatalog = catalog.asTableCatalog();
    String schema = newSchema();
    NameIdentifier tableIdent = tableIdent(schema, "partitioned_orders");
    schemas.createSchema(schema, null, Map.of());
    tableCatalog.createTable(
        tableIdent,
        partitionedColumns(),
        null,
        Map.of(),
        new Transform[] {Transforms.identity("event_day")},
        Distributions.NONE,
        SortOrders.NONE,
        Indexes.EMPTY_INDEXES);

    SupportsPartitions partitions = tableCatalog.loadTable(tableIdent).supportPartitions();
    IdentityPartition partition = identityPartition("2026-05-26");

    Partition added = partitions.addPartition(partition);
    assertEquals(partition.name(), added.name());
    assertArrayEquals(new String[] {partition.name()}, partitions.listPartitionNames());
    assertNotNull(partitions.getPartition(partition.name()));

    assertTrue(partitions.dropPartition(partition.name()));
    assertFalse(partitions.dropPartition(partition.name()));
  }

  @Test
  void testAuthenticatedCatalog() {
    containerSuite.startAuthenticatedFlussContainer();
    FlussContainer authenticatedFlussContainer = containerSuite.getAuthenticatedFlussContainer();
    String catalogName = GravitinoITUtils.genRandomName("fluss_auth_catalog");
    String schema = GravitinoITUtils.genRandomName("fluss_auth_schema");
    Catalog authenticatedCatalog = null;

    Map<String, String> properties =
        authenticatedCatalogProperties(
            authenticatedFlussContainer,
            FlussContainer.FLUSS_AUTH_USERNAME,
            FlussContainer.FLUSS_AUTH_PASSWORD);
    waitUntilConnectionSucceeds(catalogName, properties);

    try {
      authenticatedCatalog = createCatalog(catalogName, "Authenticated Fluss catalog", properties);
      Schema created =
          authenticatedCatalog
              .asSchemas()
              .createSchema(schema, "authenticated schema", Map.of("auth", "true"));
      assertEquals(schema, created.name());
      assertTrue(authenticatedCatalog.asSchemas().schemaExists(schema));
      assertTrue(authenticatedCatalog.asSchemas().dropSchema(schema, false));

      Map<String, String> wrongPasswordProperties =
          authenticatedCatalogProperties(
              authenticatedFlussContainer,
              FlussContainer.FLUSS_AUTH_USERNAME,
              FlussContainer.FLUSS_AUTH_PASSWORD + "-wrong");
      wrongPasswordProperties.put(
          CATALOG_BYPASS_PREFIX + ConfigOptions.CLIENT_REQUEST_TIMEOUT.key(), "2 s");
      assertThrows(
          ConnectionFailedException.class,
          () ->
              metalake.testConnection(
                  GravitinoITUtils.genRandomName("invalid_fluss_auth_catalog"),
                  Catalog.Type.RELATIONAL,
                  PROVIDER,
                  "Invalid authenticated Fluss catalog",
                  wrongPasswordProperties));
    } finally {
      if (authenticatedCatalog != null) {
        try {
          authenticatedCatalog.asSchemas().dropSchema(schema, true);
        } catch (Exception ignored) {
        }
      }
      try {
        metalake.dropCatalog(catalogName, true);
      } catch (Exception ignored) {
      }
    }
  }

  private void createMetalake() {
    client.createMetalake(METALAKE_NAME, "comment", Map.of());
    metalake = client.loadMetalake(METALAKE_NAME);
    assertEquals(METALAKE_NAME, metalake.name());
  }

  private Catalog createCatalog(
      String catalogName, String comment, Map<String, String> properties) {
    Catalog createdCatalog =
        metalake.createCatalog(catalogName, Catalog.Type.RELATIONAL, PROVIDER, comment, properties);
    Catalog loadedCatalog = metalake.loadCatalog(catalogName);
    assertEquals(createdCatalog, loadedCatalog);
    return loadedCatalog;
  }

  private void waitUntilConnectionSucceeds(String catalogName, Map<String, String> properties) {
    Awaitility.await()
        .atMost(2, TimeUnit.MINUTES)
        .pollInterval(1, TimeUnit.SECONDS)
        .untilAsserted(
            () ->
                assertDoesNotThrow(
                    () ->
                        metalake.testConnection(
                            catalogName,
                            Catalog.Type.RELATIONAL,
                            PROVIDER,
                            "Fluss catalog for IT",
                            properties)));
  }

  private Map<String, String> catalogProperties(FlussContainer flussContainer) {
    return Map.of(ConfigOptions.BOOTSTRAP_SERVERS.key(), flussContainer.bootstrapServers());
  }

  private Map<String, String> invalidCatalogProperties(String unusedPort) {
    Map<String, String> properties = new HashMap<>();
    properties.put(ConfigOptions.BOOTSTRAP_SERVERS.key(), "localhost:" + unusedPort);
    properties.put(CATALOG_BYPASS_PREFIX + ConfigOptions.CLIENT_REQUEST_TIMEOUT.key(), "2 s");
    return properties;
  }

  private Map<String, String> authenticatedCatalogProperties(
      FlussContainer flussContainer, String username, String password) {
    Map<String, String> properties = new HashMap<>(catalogProperties(flussContainer));
    properties.put(CATALOG_BYPASS_PREFIX + ConfigOptions.CLIENT_SECURITY_PROTOCOL.key(), "sasl");
    properties.put(CATALOG_BYPASS_PREFIX + ConfigOptions.CLIENT_SASL_MECHANISM.key(), "PLAIN");
    properties.put(CATALOG_BYPASS_PREFIX + ConfigOptions.CLIENT_SASL_JAAS_USERNAME.key(), username);
    properties.put(CATALOG_BYPASS_PREFIX + ConfigOptions.CLIENT_SASL_JAAS_PASSWORD.key(), password);
    return properties;
  }

  private String newSchema() {
    currentSchema = GravitinoITUtils.genRandomName("fluss_it_schema");
    return currentSchema;
  }

  private NameIdentifier tableIdent(String schema, String table) {
    return NameIdentifier.of(schema, table);
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
