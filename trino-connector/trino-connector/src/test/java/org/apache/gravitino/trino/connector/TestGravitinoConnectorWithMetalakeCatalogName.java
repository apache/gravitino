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
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

import io.trino.Session;
import io.trino.plugin.memory.MemoryPlugin;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorManager;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Disabled
public class TestGravitinoConnectorWithMetalakeCatalogName extends AbstractTestQueryFramework {

  GravitinoMockServer server;

  @Override
  protected QueryRunner createQueryRunner() throws Exception {
    server = closeAfterClass(new GravitinoMockServer());
    GravitinoAdminClient gravitinoClient = server.createGravitinoClient();

    Session session = testSessionBuilder().setCatalog("gravitino").build();

    try {
      DistributedQueryRunner queryRunner =
          DistributedQueryRunner.builder(session).setNodeCount(1).build();

      TestGravitinoPlugin gravitinoPlugin = new TestGravitinoPlugin(gravitinoClient);
      queryRunner.installPlugin(gravitinoPlugin);

      {
        // create a gravitino connector named gravitino using metalake test
        HashMap<String, String> properties = new HashMap<>();
        properties.put("gravitino.metalake", "test");
        properties.put("gravitino.uri", "http://127.0.0.1:8090");
        properties.put("gravitino.simplify-catalog-names", "false");
        properties.put(
            "trino.catalog.store", queryRunner.getCoordinator().getBaseDataDir().toString());
        properties.put(
            "trino.jdbc.uri",
            queryRunner.getCoordinator().getBaseUrl().toString().replace("http", "jdbc:trino"));
        queryRunner.createCatalog("gravitino", "gravitino", properties);
      }

      {
        // create a gravitino connector named test1 using metalake gravitino1
        HashMap<String, String> properties = new HashMap<>();
        properties.put("gravitino.metalake", "test1");
        properties.put("gravitino.uri", "http://127.0.0.1:8090");
        properties.put("gravitino.simplify-catalog-names", "false");
        properties.put(
            "trino.catalog.store", queryRunner.getCoordinator().getBaseDataDir().toString());
        properties.put(
            "trino.jdbc.uri",
            queryRunner.getCoordinator().getBaseUrl().toString().replace("http", "jdbc:trino"));
        queryRunner.createCatalog("gravitino1", "gravitino", properties);
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
  public void testSystemTable() throws Exception {
    MaterializedResult expectedResult = computeActual("select * from gravitino.system.catalog");
    Assertions.assertEquals(expectedResult.getRowCount(), 1);
    List<MaterializedRow> expectedRows = expectedResult.getMaterializedRows();
    MaterializedRow row = expectedRows.get(0);
    Assertions.assertEquals(row.getField(0), "memory");
    Assertions.assertEquals(row.getField(1), "memory");
    Assertions.assertEquals(row.getField(2), "{\"max_ttl\":\"10\"}");
  }

  @Test
  public void testCreateCatalog() throws Exception {
    // testing the catalogs
    assertThat(computeActual("show catalogs").getOnlyColumnAsSet()).contains("gravitino");
    assertThat(computeActual("show catalogs").getOnlyColumnAsSet()).contains("gravitino1");
    assertThat(computeActual("show catalogs").getOnlyColumnAsSet()).contains("\"test.memory\"");

    // testing the gravitino connector framework works.
    assertThat(computeActual("select * from system.jdbc.tables").getRowCount()).isGreaterThan(1);

    // test metalake named test. the connector name is gravitino
    assertUpdate("call gravitino.system.create_catalog('memory1', 'memory', Map())");
    assertThat(computeActual("show catalogs").getOnlyColumnAsSet()).contains("\"test.memory1\"");
    assertUpdate("call gravitino.system.drop_catalog('memory1')");
    assertThat(computeActual("show catalogs").getOnlyColumnAsSet())
        .doesNotContain("\"test.memory1\"");

    assertUpdate(
        "call gravitino.system.create_catalog("
            + "catalog=>'memory1', provider=>'memory', properties => Map(array['max_ttl'], array['10']), ignore_exist => true)");
    assertThat(computeActual("show catalogs").getOnlyColumnAsSet()).contains("\"test.memory1\"");

    assertUpdate(
        "call gravitino.system.drop_catalog(catalog => 'memory1', ignore_not_exist => true)");
    assertThat(computeActual("show catalogs").getOnlyColumnAsSet())
        .doesNotContain("\"test.memory1\"");

    // test metalake named test1. the connector name is gravitino1
    GravitinoAdminClient gravitinoClient = server.createGravitinoClient();
    gravitinoClient.createMetalake("test1", "", Collections.emptyMap());

    assertUpdate("call gravitino1.system.create_catalog('memory1', 'memory', Map())");
    assertThat(computeActual("show catalogs").getOnlyColumnAsSet()).contains("\"test1.memory1\"");
    assertUpdate("call gravitino1.system.drop_catalog('memory1')");
    assertThat(computeActual("show catalogs").getOnlyColumnAsSet())
        .doesNotContain("\"test1.memory1\"");
  }

  @Test
  public void testCreateTable() {
    String fullSchemaName = "\"test.memory\".db_01";
    String tableName = "tb_01";
    String fullTableName = fullSchemaName + "." + tableName;

    assertUpdate("create schema " + fullSchemaName);

    // try to get table
    assertThat(computeActual("show tables from " + fullSchemaName).getOnlyColumnAsSet())
        .doesNotContain(tableName);

    // try to create table
    assertUpdate("create table " + fullTableName + " (a varchar, b int)");
    assertThat(computeActual("show tables from " + fullSchemaName).getOnlyColumnAsSet())
        .contains(tableName);

    assertThat((String) computeScalar("show create table " + fullTableName))
        .startsWith(format("CREATE TABLE %s", fullTableName));

    // cleanup
    assertUpdate("drop table " + fullTableName);
    assertUpdate("drop schema " + fullSchemaName);
  }
}
