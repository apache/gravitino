/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

import io.trino.Session;
import io.trino.testing.BaseConnectorTest;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import java.util.HashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestGravitinoConnector extends BaseConnectorTest {

  private static final Logger LOG = LoggerFactory.getLogger(TestGravitinoConnector.class);

  @Override
  protected QueryRunner createQueryRunner() throws Exception {
    Session session = testSessionBuilder().setCatalog("gravitino").build();

    QueryRunner queryRunner = null;
    try {
      // queryRunner = LocalQueryRunner.builder(session).build();
      queryRunner = DistributedQueryRunner.builder(session).setNodeCount(1).build();

      queryRunner.installPlugin(new GravitinoPlugin());

      HashMap<String, String> properties = new HashMap<>();
      properties.put("gravitino.metalake", "test");
      queryRunner.createCatalog("gravitino", "gravitino", properties);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return queryRunner;
  }

  @Override
  public void testCreateSchema() {
    assertThat(computeActual("SHOW catalogs").getOnlyColumnAsSet()).contains("gravitino");
  }
}
