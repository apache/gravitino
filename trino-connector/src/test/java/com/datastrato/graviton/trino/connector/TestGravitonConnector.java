/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.trino.connector;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.testing.BaseConnectorTest;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import java.util.HashMap;

public class TestGravitonConnector extends BaseConnectorTest {

  private static final Logger LOG = Logger.get(TestGravitonConnector.class);

  @Override
  protected QueryRunner createQueryRunner() throws Exception {
    Session session = testSessionBuilder().setCatalog("graviton").build();

    QueryRunner queryRunner = null;
    try {
      // queryRunner = LocalQueryRunner.builder(session).build();
      queryRunner = DistributedQueryRunner.builder(session).setNodeCount(1).build();

      queryRunner.installPlugin(new GravitonPlugin());

      HashMap<String, String> properties = new HashMap<>();
      properties.put("graviton.metalake", "test");
      queryRunner.createCatalog("graviton", "graviton", properties);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return queryRunner;
  }

  @Override
  public void testCreateSchema() {
    assertThat(computeActual("SHOW CATALOGS").getOnlyColumnAsSet()).contains("graviton");
  }
}
