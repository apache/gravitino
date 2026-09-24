/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.catalog.clickhouse.integration.test;

import static org.apache.gravitino.catalog.clickhouse.ClickHouseTablePropertiesMetadata.CLICKHOUSE_PROJECTIONS_KEY;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.jdbc.config.JdbcConfig;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.integration.test.container.ClickHouseContainer;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

/**
 * Integration tests for projection metadata round-tripping across ClickHouse system-table schemas.
 */
@Tag("gravitino-docker-test")
@TestInstance(Lifecycle.PER_CLASS)
public class CatalogClickHouseProjectionIT extends BaseIT {

  private static final String CLICKHOUSE_24_9_IMAGE = "clickhouse/clickhouse-server:24.9.1.3278";
  private static final String CLICKHOUSE_25_12_IMAGE = "clickhouse/clickhouse-server:25.12.2.54";
  private static final String DATABASE = "gravitino_projection_it";
  private static final String SOURCE_TABLE = "projection_source";
  private static final String TARGET_TABLE = "projection_target";
  private static final String NO_PROJECTION_TABLE = "no_projection_source";

  private GravitinoMetalake metalake;
  private String metalakeName;

  @BeforeAll
  void createMetalake() {
    metalakeName = GravitinoITUtils.genRandomName("clickhouse_projection_it_metalake");
    client.createMetalake(
        metalakeName, "projection metadata integration test", Collections.emptyMap());
    metalake = client.loadMetalake(metalakeName);
  }

  @AfterAll
  void dropMetalake() {
    if (metalake != null) {
      client.disableMetalake(metalakeName);
      client.dropMetalake(metalakeName, true);
    }
  }

  @Test
  void testProjectionRoundTripOnClickHouse24_9WithoutSettingsColumn() throws Exception {
    runProjectionRoundTrip(CLICKHOUSE_24_9_IMAGE, false);
  }

  @Test
  void testProjectionSettingsRoundTripOnClickHouse25_12() throws Exception {
    runProjectionRoundTrip(CLICKHOUSE_25_12_IMAGE, true);
  }

  private void runProjectionRoundTrip(String image, boolean withProjectionSettings)
      throws Exception {
    try (HostMappedClickHouseContainer container = new HostMappedClickHouseContainer(image)) {
      container.start();
      String catalogName = GravitinoITUtils.genRandomName("clickhouse_projection_it_catalog");
      Catalog catalog = createCatalog(catalogName, container);
      try {
        catalog.asSchemas().createSchema(DATABASE, null, Collections.emptyMap());
        createSourceTable(container, withProjectionSettings);

        TableCatalog tableCatalog = catalog.asTableCatalog();
        NameIdentifier sourceIdentifier = NameIdentifier.of(DATABASE, SOURCE_TABLE);
        Table loaded = tableCatalog.loadTable(sourceIdentifier);
        String projectionProperty = loaded.properties().get(CLICKHOUSE_PROJECTIONS_KEY);
        Assertions.assertNotNull(projectionProperty, loaded.properties().toString());
        JsonNode definitions = new ObjectMapper().readTree(projectionProperty);
        Assertions.assertEquals(4, definitions.size());
        JsonNode orderedProjection = null;
        JsonNode aggregateProjection = null;
        Set<String> projectionNames = new HashSet<>();
        for (JsonNode definition : definitions) {
          String projectionName = definition.get("name").asText();
          projectionNames.add(projectionName);
          if ("by date".equals(projectionName)) {
            orderedProjection = definition;
          } else if ("by_region".equals(projectionName)) {
            aggregateProjection = definition;
          }
        }
        Assertions.assertEquals(
            Set.of("by date", "by_region", "by expression", "by_customer"), projectionNames);
        Assertions.assertNotNull(orderedProjection);
        Assertions.assertEquals("Normal", orderedProjection.get("type").asText());
        Assertions.assertNotNull(aggregateProjection);
        Assertions.assertEquals("Aggregate", aggregateProjection.get("type").asText());
        if (withProjectionSettings) {
          Assertions.assertEquals(
              "128", orderedProjection.get("settings").get("index_granularity").asText());
        } else {
          Assertions.assertTrue(orderedProjection.get("settings").isEmpty());
        }

        NameIdentifier targetIdentifier = NameIdentifier.of(DATABASE, TARGET_TABLE);
        Table recreated =
            tableCatalog.createTable(
                targetIdentifier,
                loaded.columns(),
                loaded.comment(),
                loaded.properties(),
                loaded.partitioning(),
                loaded.distribution(),
                loaded.sortOrder(),
                loaded.index());
        Assertions.assertEquals(TARGET_TABLE, recreated.name());

        Table loadedTarget = tableCatalog.loadTable(targetIdentifier);
        Assertions.assertEquals(
            projectionProperty, loadedTarget.properties().get(CLICKHOUSE_PROJECTIONS_KEY));
        String sourceCreate = readCreateTable(container, SOURCE_TABLE);
        String targetCreate = readCreateTable(container, TARGET_TABLE);
        for (String projectionName : projectionNames) {
          Assertions.assertTrue(sourceCreate.contains(projectionName), sourceCreate);
          Assertions.assertTrue(targetCreate.contains(projectionName), targetCreate);
        }
        Assertions.assertTrue(sourceCreate.contains("PROJECTION"), sourceCreate);
        Assertions.assertTrue(targetCreate.contains("PROJECTION"), targetCreate);
        if (withProjectionSettings) {
          Assertions.assertTrue(targetCreate.contains("index_granularity"), targetCreate);
        }
        List<List<String>> sourceRows =
            readProjectionRows(container, SOURCE_TABLE, withProjectionSettings);
        List<List<String>> targetRows =
            readProjectionRows(container, TARGET_TABLE, withProjectionSettings);
        Assertions.assertEquals(4, sourceRows.size());
        Assertions.assertEquals(sourceRows, targetRows);

        Table withoutProjections =
            tableCatalog.loadTable(NameIdentifier.of(DATABASE, NO_PROJECTION_TABLE));
        Assertions.assertFalse(
            withoutProjections.properties().containsKey(CLICKHOUSE_PROJECTIONS_KEY));
      } finally {
        metalake.disableCatalog(catalogName);
        metalake.dropCatalog(catalogName, true);
      }
    }
  }

  private Catalog createCatalog(String catalogName, HostMappedClickHouseContainer container) {
    Map<String, String> properties =
        Map.of(
            JdbcConfig.JDBC_URL.getKey(), container.getJdbcUrl(),
            JdbcConfig.JDBC_DRIVER.getKey(), "com.clickhouse.jdbc.ClickHouseDriver",
            JdbcConfig.USERNAME.getKey(), container.getUsername(),
            JdbcConfig.PASSWORD.getKey(), container.getPassword());
    metalake.createCatalog(
        catalogName, Catalog.Type.RELATIONAL, "jdbc-clickhouse", "projection catalog", properties);
    return metalake.loadCatalog(catalogName);
  }

  private static void createSourceTable(
      ClickHouseContainer container, boolean withProjectionSettings) throws SQLException {
    String customSettings =
        withProjectionSettings ? " WITH SETTINGS (index_granularity = 128)" : "";
    String createTable =
        "CREATE TABLE "
            + DATABASE
            + "."
            + SOURCE_TABLE
            + " ("
            + "id UInt64, event_date Date, region String, customer String, "
            + "PROJECTION `by date` (SELECT event_date, id ORDER BY event_date, id)"
            + customSettings
            + ", PROJECTION by_region (SELECT region, count() GROUP BY region)"
            + ", PROJECTION `by expression` (SELECT lower(concat(customer, 'O''Reilly')) "
            + "ORDER BY lower(concat(customer, 'O''Reilly')))"
            + ") ENGINE = MergeTree ORDER BY id";
    String createTableWithoutProjections =
        "CREATE TABLE "
            + DATABASE
            + "."
            + NO_PROJECTION_TABLE
            + " (id UInt64) ENGINE = MergeTree ORDER BY id";
    String addProjection =
        "ALTER TABLE "
            + DATABASE
            + "."
            + SOURCE_TABLE
            + " ADD PROJECTION by_customer (SELECT customer ORDER BY customer)";

    try (Connection connection =
            DriverManager.getConnection(
                container.getJdbcUrl(), container.getUsername(), container.getPassword());
        Statement statement = connection.createStatement()) {
      statement.execute(createTable);
      statement.execute(addProjection);
      statement.execute(createTableWithoutProjections);
    }
  }

  private static List<List<String>> readProjectionRows(
      ClickHouseContainer container, String table, boolean includeSettings) throws SQLException {
    String query =
        "SELECT name, type, query"
            + (includeSettings ? ", toJSONString(settings) AS settings_json" : "")
            + " FROM system.projections WHERE database = ? AND table = ? ORDER BY name";
    List<List<String>> rows = new ArrayList<>();
    try (Connection connection =
            DriverManager.getConnection(
                container.getJdbcUrl(), container.getUsername(), container.getPassword());
        PreparedStatement statement = connection.prepareStatement(query)) {
      statement.setString(1, DATABASE);
      statement.setString(2, table);
      try (ResultSet resultSet = statement.executeQuery()) {
        while (resultSet.next()) {
          rows.add(
              List.of(
                  resultSet.getString("name"),
                  resultSet.getString("type"),
                  resultSet.getString("query"),
                  includeSettings ? resultSet.getString("settings_json") : "{}"));
        }
      }
    }
    return rows;
  }

  private static String readCreateTable(ClickHouseContainer container, String table)
      throws SQLException {
    String query = "SHOW CREATE TABLE " + DATABASE + "." + table;
    try (Connection connection =
            DriverManager.getConnection(
                container.getJdbcUrl(), container.getUsername(), container.getPassword());
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(query)) {
      Assertions.assertTrue(resultSet.next(), query);
      return resultSet.getString(1);
    }
  }

  private static final class HostMappedClickHouseContainer extends ClickHouseContainer {
    private HostMappedClickHouseContainer(String image) {
      super(
          image,
          "gravitino-projection-it",
          Set.of(CLICKHOUSE_PORT),
          Map.of(),
          Map.of(),
          Map.of("CLICKHOUSE_PASSWORD", PASSWORD),
          Optional.empty());
    }

    @Override
    public String getJdbcUrl() {
      return "jdbc:clickhouse:http://%s:%d?compress=0"
          .formatted(getContainer().getHost(), getMappedPort(CLICKHOUSE_PORT));
    }
  }
}
