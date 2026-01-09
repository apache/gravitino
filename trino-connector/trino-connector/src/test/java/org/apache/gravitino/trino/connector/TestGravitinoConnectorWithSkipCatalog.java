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
package org.apache.gravitino.trino.connector;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.apache.gravitino.Catalog.Type.RELATIONAL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import io.trino.Session;
import io.trino.plugin.memory.MemoryPlugin;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorManager;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

public class TestGravitinoConnectorWithSkipCatalog extends AbstractTestQueryFramework {

  GravitinoMockServer server;

  @Override
  protected QueryRunner createQueryRunner() throws Exception {
    GravitinoAdminClient gravitinoClient = initGravitinoMockServer();
    Session session = testSessionBuilder().setCatalog("gravitino").build();

    try {
      DistributedQueryRunner queryRunner =
          DistributedQueryRunner.builder(session).setNodeCount(1).build();

      TestGravitinoPlugin gravitinoPlugin = new TestGravitinoPlugin(gravitinoClient);
      queryRunner.installPlugin(gravitinoPlugin);

      {
        // create a gravitino connector with single metalake
        HashMap<String, String> properties = new HashMap<>();
        properties.put("gravitino.metalake", "test1");
        properties.put("gravitino.uri", "http://127.0.0.1:8090");
        properties.put("gravitino.trino.skip-catalog-patterns", "a.*, b1");
        properties.put(
            "catalog.config-dir", queryRunner.getCoordinator().getBaseDataDir().toString());
        properties.put("discovery.uri", queryRunner.getCoordinator().getBaseUrl().toString());
        queryRunner.createCatalog("gravitino", "gravitino", properties);
      }

      GravitinoConnectorPluginManager.instance(this.getClass().getClassLoader())
          .installPlugin("memory", new MemoryPlugin());
      CatalogConnectorManager catalogConnectorManager =
          gravitinoPlugin.getCatalogConnectorManager();
      server.setCatalogConnectorManager(catalogConnectorManager);

      // Wait for the catalog to be created. Wait for at least 30 seconds.
      Awaitility.await()
          .atMost(30, TimeUnit.SECONDS)
          .pollInterval(1, TimeUnit.SECONDS)
          .until(() -> !catalogConnectorManager.getCatalogs().isEmpty());
      return queryRunner;

    } catch (Exception e) {
      throw new RuntimeException("Create query runner failed", e);
    }
  }

  @Test
  public void testShowCatalogsFilteredBySkipPatterns() throws Exception {
    MaterializedResult expectedResult = computeActual("show catalogs");
    assertEquals(expectedResult.getMaterializedRows().size(), 3);
    List<String> catalogs =
        expectedResult.getMaterializedRows().stream()
            .map(row -> (String) row.getField(0))
            .collect(Collectors.toList());
    assertFalse(catalogs.contains("a1"));
    assertFalse(catalogs.contains("b1"));
    assertTrue(catalogs.contains("b2"));
  }

  @Test
  public void testSystemTableQueryFilteredBySkipPatterns() throws Exception {
    MaterializedResult expectedResult = computeActual("select * from gravitino.system.catalog");
    assertEquals(expectedResult.getMaterializedRows().size(), 1);
    List<String> catalogs =
        expectedResult.getMaterializedRows().stream()
            .map(row -> (String) row.getField(0))
            .collect(Collectors.toList());
    assertFalse(catalogs.contains("a1"));
    assertFalse(catalogs.contains("b1"));
    assertTrue(catalogs.contains("b2"));
  }

  private GravitinoAdminClient initGravitinoMockServer() {
    GravitinoMockServer gravitinoMockServer = new GravitinoMockServer();
    server = closeAfterClass(gravitinoMockServer);
    GravitinoAdminClient gravitinoClient = server.createGravitinoClient();

    gravitinoClient.createMetalake("test1", "", Collections.emptyMap());
    GravitinoMetalake metalake = gravitinoClient.loadMetalake("test1");
    metalake.createCatalog("a1", RELATIONAL, "", "", Collections.emptyMap());
    metalake.createCatalog("b1", RELATIONAL, "", "", Collections.emptyMap());
    metalake.createCatalog("b2", RELATIONAL, "", "", Collections.emptyMap());
    return gravitinoClient;
  }
}
