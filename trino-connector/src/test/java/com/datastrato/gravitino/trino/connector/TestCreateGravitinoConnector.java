/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastrato.gravitino.client.GravitinoAdminClient;
import io.trino.Session;
import io.trino.plugin.memory.MemoryPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import java.util.HashMap;
import org.testng.annotations.Test;

public class TestCreateGravitinoConnector {

  GravitinoMockServer server;

  @Test
  public void testCreateSimpleCatalogNameConnector() throws Exception {
    server = new GravitinoMockServer(true);
    Session session = testSessionBuilder().setCatalog("gravitino").build();
    QueryRunner queryRunner = DistributedQueryRunner.builder(session).setNodeCount(1).build();

    GravitinoAdminClient gravitinoClient = server.createGravitinoClient();
    TestGravitinoPlugin gravitinoPlugin = new TestGravitinoPlugin();
    gravitinoPlugin.setGravitinoClient(gravitinoClient);
    queryRunner.installPlugin(gravitinoPlugin);
    queryRunner.installPlugin(new MemoryPlugin());

    {
      // create a gravitino connector named gravitino using metalake test
      HashMap<String, String> properties = new HashMap<>();
      properties.put("gravitino.metalake", "test");
      properties.put("gravitino.uri", "http://127.0.0.1:8090");
      properties.put("gravitino.simplify-catalog-names", "true");
      queryRunner.createCatalog("test0", "gravitino", properties);
    }

    {
      // Test failed to create catalog with different metalake
      HashMap<String, String> properties = new HashMap<>();
      properties.put("gravitino.metalake", "test1");
      properties.put("gravitino.uri", "http://127.0.0.1:8090");
      try {
        queryRunner.createCatalog("test1", "gravitino", properties);
      } catch (Exception e) {
        assertThat(e.getMessage()).contains("Multiple metalakes are not supported");
      }
    }

    server.close();
  }
}
