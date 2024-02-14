/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

import com.datastrato.gravitino.NameIdentifier;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.shaded.com.google.common.base.Preconditions;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Parameters({"-Xmx4G"})
public class TestGravitinoConnector extends AbstractTestQueryFramework {

  private static final Logger LOG = LoggerFactory.getLogger(TestGravitinoConnector.class);

  GravitinoMockServer server;

  @BeforeMethod
  public void reloadCatalog() {
    server.reloadCatalogs();
  }

  @Override
  protected QueryRunner createQueryRunner() throws Exception {
    server = closeAfterClass(new GravitinoMockServer());
    GravitinoPlugin.gravitinoClient = server.createGravitinoClient();

    Session session = testSessionBuilder().setCatalog("gravitino").build();
    QueryRunner queryRunner = null;
    try {
      // queryRunner = LocalQueryRunner.builder(session).build();
      queryRunner = DistributedQueryRunner.builder(session).setNodeCount(1).build();

      queryRunner.installPlugin(new GravitinoPlugin());
      queryRunner.installPlugin(new MemoryPlugin());

      {
        // create a gravitino connector named gravitino using metalake test
        HashMap<String, String> properties = new HashMap<>();
        properties.put("gravitino.metalake", "test");
        properties.put("gravitino.uri", "http://127.0.0.1:8090");
        queryRunner.createCatalog("gravitino", "gravitino", properties);
      }

      {
        // create a gravitino connector named test1 using metalake test1
        HashMap<String, String> properties = new HashMap<>();
        properties.put("gravitino.metalake", "test1");
        properties.put("gravitino.uri", "http://127.0.0.1:8090");
        queryRunner.createCatalog("test1", "gravitino", properties);
      }
      server.setCatalogConnectorManager(GravitinoPlugin.catalogConnectorManager);
      // Wait for the catalog to be created. Wait for at least 30 seconds.
      int max_tries = 35;
      while (GravitinoPlugin.catalogConnectorManager.getCatalogs().isEmpty() && max_tries > 0) {
        Thread.sleep(1000);
        max_tries--;
      }

      if (max_tries == 0) {
        throw new RuntimeException("Failed to create catalog in about 35 seconds...");
      }

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return queryRunner;
  }

  @Test
  public void testCreateSchema() {
    String catalogName = "test.memory";
    String schemaName = "db_01";
    String fullSchemaName = String.format("\"%s\".%s", catalogName, schemaName);
    assertThat(computeActual("show schemas from \"test.memory\"").getOnlyColumnAsSet())
        .doesNotContain(schemaName);

    assertUpdate("create schema " + fullSchemaName);
    assertThat(computeActual("show schemas from \"test.memory\"").getOnlyColumnAsSet())
        .contains(schemaName);

    assertThat((String) computeScalar("show create schema " + fullSchemaName))
        .startsWith(format("CREATE SCHEMA %s", fullSchemaName));

    // try to create duplicate schema
    assertQueryFails(
        "create schema " + fullSchemaName, format("line 1:1: Schema .* already exists"));

    // cleanup
    assertUpdate("drop schema " + fullSchemaName);

    // verify DROP SCHEMA for non-existing schema
    assertQueryFails("drop schema " + fullSchemaName, format("line 1:1: Schema .* does not exist"));
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
    assertUpdate("drop table" + fullTableName);
    assertUpdate("drop schema " + fullSchemaName);
  }

  @Test
  public void testInsert() throws Exception {
    String fullTableName = "\"test.memory\".db_01.tb_01";
    createTestTable(fullTableName);
    // insert some data.
    assertUpdate(String.format("insert into %s (a, b) values ('ice', 12)", fullTableName), 1);

    // select data from the table.
    MaterializedResult expectedResult = computeActual("select * from " + fullTableName);
    assertEquals(expectedResult.getRowCount(), 1);
    List<MaterializedRow> expectedRows = expectedResult.getMaterializedRows();
    MaterializedRow row = expectedRows.get(0);
    assertEquals(row.getField(0), "ice");
    assertEquals(row.getField(1), 12);

    // cleanup
    dropTestTable(fullTableName);
  }

  @Test
  public void testInsertIntoSelect() throws Exception {
    String fullTableName1 = "\"test.memory\".db_01.tb_01";
    String fullTableName2 = "\"test.memory\".db_01.tb_02";
    createTestTable(fullTableName1);
    createTestTable(fullTableName2);

    // Prepare source data
    assertUpdate(
        String.format("insert into %s (a, b) values ('Tom', 12), ('Jerry', 18)", fullTableName1),
        2);

    // Insert into select from source data
    assertUpdate(
        String.format("insert into %s (a, b) select * from %s", fullTableName2, fullTableName1), 2);
    dropTestTable(fullTableName1);
    dropTestTable(fullTableName2);
  }

  @Test
  public void testAlterTable() throws Exception {
    String fullTableName1 = "\"test.memory\".db_01.tb_01";
    String fullTableName2 = "\"test.memory\".db_01.tb_02";
    createTestTable(fullTableName1);

    // test rename table
    assertUpdate(String.format("alter table %s rename to %s", fullTableName1, fullTableName2));
    assertUpdate(
        String.format("alter table if exists %s rename to %s", fullTableName2, fullTableName1));
    dropTestTable(fullTableName1);

    createTestTable(fullTableName1);

    // test add column and drop column, but the memory connector is not supported these operations.
    assertQueryFails(
        String.format("alter table %s add column if not exists c varchar", fullTableName1),
        format("This connector does not support adding columns"));

    assertQueryFails(
        String.format("alter table %s drop column a", fullTableName1),
        format("This connector does not support dropping columns"));

    // test set table comment
    assertUpdate(String.format("comment on table %s is 'test table comments'", fullTableName1));
    assertThat((String) computeScalar("show create table " + fullTableName1))
        .contains("COMMENT 'test table comments'");

    // test rename column, but the memory connector is not supported these operations.
    assertQueryFails(
        String.format("alter table %s rename column a to c ", fullTableName1),
        format("This connector does not support renaming columns"));

    assertQueryFails(
        String.format("alter table %s alter column a set DATA TYPE int", fullTableName1),
        format("This connector does not support setting column types"));

    // test set column comment
    assertUpdate(String.format("comment on column %s.a is 'test column comments'", fullTableName1));
    assertThat((String) computeScalar("show create table " + fullTableName1))
        .contains("COMMENT 'test column comments'");

    // test set table properties, but the memory connector is not supported these operations.
    assertQueryFails(
        String.format("alter table %s set properties \"max_ttl\" = 20", fullTableName1),
        format("This connector does not support setting table properties"));

    dropTestTable(fullTableName1);
  }

  @Test
  public void testCreateCatalog() throws Exception {
    // testing the catalogs
    assertThat(computeActual("show catalogs").getOnlyColumnAsSet()).contains("gravitino");
    assertThat(computeActual("show catalogs").getOnlyColumnAsSet()).contains("test1");
    assertThat(computeActual("show catalogs").getOnlyColumnAsSet()).contains("test.memory");

    // testing the gravitino connector framework works.
    assertThat(computeActual("select * from system.jdbc.tables"));

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

    // test metalake named test1. the connnector name is test1
    GravitinoPlugin.gravitinoClient.createMetalake(
        NameIdentifier.ofMetalake("test1"), "", Collections.emptyMap());

    assertUpdate("call test1.system.create_catalog('memory1', 'memory', Map())");
    assertThat(computeActual("show catalogs").getOnlyColumnAsSet()).contains("test1.memory1");
    assertUpdate("call test1.system.drop_catalog('memory1')");
    assertThat(computeActual("show catalogs").getOnlyColumnAsSet()).doesNotContain("test1.memory1");
  }

  private TableName createTestTable(String fullTableName) throws Exception {
    TableName tableName = new TableName(fullTableName);

    // create schema and table
    assertUpdate("create schema if not exists " + tableName.fullSchemaName());
    assertUpdate("create table " + fullTableName + " (a varchar, b int)");
    return tableName;
  }

  private void dropTestTable(String fullTableName) throws Exception {
    TableName tableName = new TableName(fullTableName);
    assertUpdate("drop table " + tableName.fullTableName());
    boolean emptyTable =
        computeActual("show tables from " + tableName.fullSchemaName())
            .getMaterializedRows()
            .isEmpty();
    if (emptyTable) assertUpdate("drop schema" + tableName.fullSchemaName());
  }

  static class TableName {
    String catalog;
    String schema;
    String table;

    String fullSchemaName() {
      return "\"" + catalog + "\"" + "." + schema;
    }

    String fullTableName() {
      return "\"" + catalog + "\"" + "." + schema + "." + table;
    }

    TableName(String fullTableName) {
      String regex = "\"([^\"]*)\"\\.([^\\.]+)\\.([^\\.]+)";
      Pattern pattern = Pattern.compile(regex);

      Matcher matcher = pattern.matcher(fullTableName);
      Preconditions.checkArgument(matcher.find(), "Invalid table name: " + fullTableName);
      catalog = matcher.group(1);
      schema = matcher.group(2);
      table = matcher.group(3);
    }
  }
}
