/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastrato.gravitino.client.GravitinoAdminClient;
import com.datastrato.gravitino.trino.connector.catalog.CatalogConnectorManager;
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
    QueryRunner queryRunner;
    try {
      queryRunner = DistributedQueryRunner.builder(session).setNodeCount(1).build();

      TestGravitinoPlugin gravitinoPlugin = new TestGravitinoPlugin(gravitinoClient);
      queryRunner.installPlugin(gravitinoPlugin);
      queryRunner.installPlugin(new MemoryPlugin());

      {
        // create a gravitino connector named gravitino using metalake test
        HashMap<String, String> properties = new HashMap<>();
        properties.put("gravitino.metalake", "test");
        properties.put("gravitino.uri", "http://127.0.0.1:8090");
        properties.put("gravitino.simplify-catalog-names", "false");
        queryRunner.createCatalog("gravitino", "gravitino", properties);
      }

      {
        // create a gravitino connector named test1 using metalake test1
        HashMap<String, String> properties = new HashMap<>();
        properties.put("gravitino.metalake", "test1");
        properties.put("gravitino.uri", "http://127.0.0.1:8090");
        properties.put("gravitino.simplify-catalog-names", "false");
        queryRunner.createCatalog("test1", "gravitino", properties);
      }

      CatalogConnectorManager catalogConnectorManager =
          gravitinoPlugin.getCatalogConnectorManager();
      server.setCatalogConnectorManager(catalogConnectorManager);

      // test the catalog has loaded
      Assertions.assertFalse(catalogConnectorManager.getCatalogs().isEmpty());
    } catch (Exception e) {
      throw new RuntimeException("Create query runner failed", e);
    }
    return queryRunner;
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
    assertThat(computeActual("show catalogs").getOnlyColumnAsSet()).contains("test1");
    assertThat(computeActual("show catalogs").getOnlyColumnAsSet()).contains("test.memory");

    // testing the gravitino connector framework works.
    assertThat(computeActual("select * from system.jdbc.tables").getRowCount()).isGreaterThan(1);

    // test metalake named test. the connector name is gravitino
    assertUpdate("call gravitino.system.create_catalog('memory1', 'memory', Map())");
    assertThat(computeActual("show catalogs").getOnlyColumnAsSet()).contains("test.memory1");
    assertUpdate("call gravitino.system.drop_catalog('memory1')");
    assertThat(computeActual("show catalogs").getOnlyColumnAsSet()).doesNotContain("test.memory1");

    assertUpdate(
        "call gravitino.system.create_catalog("
            + "catalog=>'memory1', provider=>'memory', properties => Map(array['max_ttl'], array['10']), ignore_exist => true)");
    assertThat(computeActual("show catalogs").getOnlyColumnAsSet()).contains("test.memory1");

    assertUpdate(
        "call gravitino.system.drop_catalog(catalog => 'memory1', ignore_not_exist => true)");
    assertThat(computeActual("show catalogs").getOnlyColumnAsSet()).doesNotContain("test.memory1");

    // test metalake named test1. the connector name is test1
    GravitinoAdminClient gravitinoClient = server.createGravitinoClient();
    gravitinoClient.createMetalake("test1", "", Collections.emptyMap());

    assertUpdate("call test1.system.create_catalog('memory1', 'memory', Map())");
    assertThat(computeActual("show catalogs").getOnlyColumnAsSet()).contains("test1.memory1");
    assertUpdate("call test1.system.drop_catalog('memory1')");
    assertThat(computeActual("show catalogs").getOnlyColumnAsSet()).doesNotContain("test1.memory1");
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
