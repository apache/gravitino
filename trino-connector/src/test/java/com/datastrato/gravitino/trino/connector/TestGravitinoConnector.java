/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector;

import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

import com.datastrato.gravitino.trino.connector.util.MockGravitinoServer;
import io.trino.Session;
import io.trino.plugin.memory.MemoryConnector;
import io.trino.plugin.memory.MemoryPlugin;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Parameters({"-Xmx1G"})
public class TestGravitinoConnector extends AbstractTestQueryFramework {

  private static final Logger LOG = LoggerFactory.getLogger(TestGravitinoConnector.class);

  MockGravitinoServer server;
  MemoryConnector memoryConnector;

  private int initGravitinoServer() throws Exception {
    server = closeAfterClass(new MockGravitinoServer());
    server.start(0);
    return server.getLocalPort();
  }

  @Override
  protected QueryRunner createQueryRunner() throws Exception {
    int port = initGravitinoServer();

    Session session = testSessionBuilder().setCatalog("gravitino").build();
    QueryRunner queryRunner = null;
    try {
      // queryRunner = LocalQueryRunner.builder(session).build();
      queryRunner = DistributedQueryRunner.builder(session).setNodeCount(1).build();

      queryRunner.installPlugin(new GravitinoPlugin());
      queryRunner.installPlugin(new MemoryPlugin());

      HashMap<String, String> properties = new HashMap<>();
      properties.put("gravitino.metalake", "test");
      properties.put("gravitino.uri", "http://127.0.0.1:" + port);

      queryRunner.createCatalog("gravitino", "gravitino", properties);

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return queryRunner;
  }

  @Test
  public void testCreateSchema() {
    String catalogName = "test.memory";
    // testing the catalogs
    assertThat(computeActual("show catalogs").getOnlyColumnAsSet()).contains("gravitino");
    assertThat(computeActual("show catalogs").getOnlyColumnAsSet()).contains(catalogName);

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
    String schemaName = "db_01";
    String fullSchemaName = "\"test.memory\".db_01";
    String tableName = "tb_01";
    String fullTableName = fullSchemaName + "." + tableName;

    // preparing internal connector metadata.
    preparingTestingData(schemaName, tableName);

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

    cleanTestingData(schemaName);
  }

  private void preparingTestingData(String schemaName, String tableName) {
    memoryConnector = (MemoryConnector) GravitinoPlugin.internalTestingConnector;

    // create schema
    ConnectorMetadata metadata = memoryConnector.getMetadata(null, null);
    metadata.createSchema(null, schemaName, emptyMap(), null);

    // create table
    SchemaTableName schemaTableName = new SchemaTableName(schemaName, tableName);
    ArrayList<ColumnMetadata> columnMetadataList = new ArrayList<>();
    columnMetadataList.add(new ColumnMetadata("a", VARCHAR));
    columnMetadataList.add(new ColumnMetadata("b", INTEGER));

    ConnectorTableMetadata connectorTableMetadata =
        new ConnectorTableMetadata(
            schemaTableName, columnMetadataList, emptyMap(), Optional.of(""));
    metadata.createTable(null, connectorTableMetadata, false);
  }

  private void cleanTestingData(String schemaName) {
    memoryConnector = (MemoryConnector) GravitinoPlugin.internalTestingConnector;
    ConnectorMetadata metadata = memoryConnector.getMetadata(null, null);
    // drop schema and tables
    metadata.dropSchema(null, schemaName, true);
  }

  @Test
  public void testInsert() {
    String schemaName = "db_01";
    String fullSchemaName = "\"test.memory\".db_01";
    String tableName = "tb_01";
    String fullTableName = fullSchemaName + "." + tableName;

    // preparing internal connector metadata.
    preparingTestingData(schemaName, tableName);

    // create schema and table
    assertUpdate("create schema \"test.memory\".db_01");
    assertUpdate("create table " + fullTableName + " (a varchar, b int)");

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
    assertUpdate("drop schema " + fullSchemaName + " cascade");
    cleanTestingData(schemaName);
  }
}
