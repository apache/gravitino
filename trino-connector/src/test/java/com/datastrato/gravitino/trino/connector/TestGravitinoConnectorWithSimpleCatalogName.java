/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.client.GravitinoAdminClient;
import com.datastrato.gravitino.trino.connector.catalog.CatalogConnectorManager;
import io.trino.Session;
import io.trino.plugin.memory.MemoryPlugin;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testng.annotations.Test;

public class TestGravitinoConnectorWithSimpleCatalogName extends AbstractTestQueryFramework {

  GravitinoMockServer server;

  @Override
  protected QueryRunner createQueryRunner() throws Exception {
    server = closeAfterClass(new GravitinoMockServer(true));
    GravitinoAdminClient gravitinoClient = server.createGravitinoClient();

    Session session = testSessionBuilder().setCatalog("gravitino").build();
    QueryRunner queryRunner = null;
    try {
      queryRunner = DistributedQueryRunner.builder(session).setNodeCount(1).build();

      TestGravitinoPlugin gravitinoPlugin = new TestGravitinoPlugin(gravitinoClient);
      queryRunner.installPlugin(gravitinoPlugin);
      queryRunner.installPlugin(new MemoryPlugin());

      // create a gravitino connector named gravitino using metalake test
      HashMap<String, String> properties = new HashMap<>();
      properties.put("gravitino.metalake", "test");
      properties.put("gravitino.uri", "http://127.0.0.1:8090");
      properties.put("gravitino.simplify-catalog-names", "true");
      queryRunner.createCatalog("gravitino", "gravitino", properties);

      CatalogConnectorManager catalogConnectorManager =
          gravitinoPlugin.getCatalogConnectorManager();
      server.setCatalogConnectorManager(catalogConnectorManager);
      // Wait for the catalog to be created. Wait for at least 30 seconds.
      Awaitility.await()
          .atMost(30, TimeUnit.SECONDS)
          .pollInterval(1, TimeUnit.SECONDS).until(
              () -> !catalogConnectorManager.getCatalogs().isEmpty());
    } catch (Exception e) {
      throw new RuntimeException("Create query runner failed", e);
    }
    return queryRunner;
  }

  @Test
  public void testCatalogName() {
    assertThat(computeActual("show catalogs").getOnlyColumnAsSet()).contains("gravitino");
    assertThat(computeActual("show catalogs").getOnlyColumnAsSet()).contains("memory");
    assertUpdate("call gravitino.system.create_catalog('memory1', 'memory', Map())");
    assertThat(computeActual("show catalogs").getOnlyColumnAsSet()).contains("memory1");

    String schemaName = "db1";
    String fullSchemaName = String.format("\"%s\".%s", "memory", schemaName);
    assertUpdate("create schema " + fullSchemaName);
    assertThat(computeActual("show schemas from \"memory\"").getOnlyColumnAsSet())
        .contains(schemaName);

    assertUpdate("drop schema " + fullSchemaName);
    assertUpdate("call gravitino.system.drop_catalog('memory1')");
  }

  @Test
  public void testSystemTable() throws Exception {
    MaterializedResult expectedResult = computeActual("select * from gravitino.system.catalog");
    assertEquals(expectedResult.getRowCount(), 1);
    List<MaterializedRow> expectedRows = expectedResult.getMaterializedRows();
    MaterializedRow row = expectedRows.get(0);
    assertEquals(row.getField(0), "memory");
    assertEquals(row.getField(1), "memory");
    assertEquals(row.getField(2), "{\"max_ttl\":\"10\"}");
  }
}
