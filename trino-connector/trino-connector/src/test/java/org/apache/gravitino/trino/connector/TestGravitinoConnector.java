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

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.google.common.base.Preconditions;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.junit.jupiter.api.Test;

public abstract class TestGravitinoConnector extends AbstractGravitinoConnectorTest {

  public static final int SPI_VERSION_TEST_SUPPORT_RENAME_COLUMN = 452;
  public static final int SPI_VERSION_SUPPORT_ADD_COLUMN_WITH_POSITION = 468;
  public static final int SPI_VERSION_TEST_SUPPORT_ADD_COLUMN = 452;

  @Override
  protected void configureCatalogs(
      DistributedQueryRunner queryRunner, GravitinoAdminClient gravitinoClient) {
    // create a gravitino connector named gravitino using metalake test
    HashMap<String, String> properties = new HashMap<>();
    properties.put("gravitino.metalake", "test");
    properties.put("gravitino.uri", "http://127.0.0.1:8090");
    properties.put("catalog.config-dir", queryRunner.getCoordinator().getBaseDataDir().toString());
    properties.put("discovery.uri", queryRunner.getCoordinator().getBaseUrl().toString());
    queryRunner.createCatalog("gravitino", "gravitino", properties);
  }

  @Test
  public void testCreateSchema() {
    String catalogName = "memory";
    String schemaName = "db_01";
    String fullSchemaName = String.format("%s.%s", catalogName, schemaName);
    assertThat(computeActual("show schemas from " + catalogName).getOnlyColumnAsSet())
        .doesNotContain(schemaName);

    assertUpdate("create schema " + fullSchemaName);
    assertThat(computeActual("show schemas from \"memory\"").getOnlyColumnAsSet())
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
    String fullSchemaName = "memory.db_01";
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

  @Test
  public void testInsert() throws Exception {
    String fullTableName = "\"memory\".db_01.tb_01";
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
    String fullTableName1 = "\"memory\".db_01.tb_01";
    String fullTableName2 = "\"memory\".db_01.tb_02";
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
  public void testCreateTableAsSelect() throws Exception {
    String sourceTable = "\"memory\".db_01.tb_src";
    String ctasTable = "\"memory\".db_01.tb_ctas";

    createTestTable(sourceTable);

    // Prepare source data
    assertUpdate(
        String.format(
            "insert into %s (a, b) values ('Alice', 1), ('Bob', 2), ('Charlie', 3)", sourceTable),
        3);

    // Create table as select (CTAS)
    assertUpdate(String.format("create table %s as select * from %s", ctasTable, sourceTable), 3);

    // Verify the CTAS table contains the expected data
    MaterializedResult result = computeActual("select * from " + ctasTable);
    assertEquals(result.getRowCount(), 3);

    // Verify schema matches
    assertThat((String) computeScalar("show create table " + ctasTable))
        .contains("a varchar")
        .contains("b integer");

    // Cleanup
    dropTestTable(ctasTable);
    dropTestTable(sourceTable);
  }

  @Test
  public void testCreateTableAsSelectEmpty() throws Exception {
    String sourceTable = "\"memory\".db_01.tb_src_empty";
    String ctasTable = "\"memory\".db_01.tb_ctas_empty";

    createTestTable(sourceTable);

    // Create table as select from an empty source table
    assertUpdate(String.format("create table %s as select * from %s", ctasTable, sourceTable), 0);

    // Verify the CTAS table is empty but exists with the correct schema
    MaterializedResult result = computeActual("select * from " + ctasTable);
    assertEquals(result.getRowCount(), 0);

    assertThat((String) computeScalar("show create table " + ctasTable))
        .contains("a varchar")
        .contains("b integer");

    // Cleanup
    dropTestTable(ctasTable);
    dropTestTable(sourceTable);
  }

  @Test
  public void testAlterTable() throws Exception {
    String fullTableName1 = "\"memory\".db_01.tb_01";
    String fullTableName2 = "\"memory\".db_01.tb_02";
    createTestTable(fullTableName1);

    // test rename table
    assertUpdate(String.format("alter table %s rename to %s", fullTableName1, fullTableName2));
    assertUpdate(
        String.format("alter table if exists %s rename to %s", fullTableName2, fullTableName1));
    dropTestTable(fullTableName1);

    createTestTable(fullTableName1);

    // test set table comment
    assertUpdate(String.format("comment on table %s is 'test table comments'", fullTableName1));
    assertThat((String) computeScalar("show create table " + fullTableName1))
        .contains("COMMENT 'test table comments'");

    // test set column comment
    assertUpdate(String.format("comment on column %s.a is 'test column comments'", fullTableName1));
    assertThat((String) computeScalar("show create table " + fullTableName1))
        .contains("COMMENT 'test column comments'");

    // test add column and drop column, but the memory connector is not supported these operations.
    if (trinoVersion < SPI_VERSION_TEST_SUPPORT_ADD_COLUMN) {
      assertQueryFails(
          String.format("alter table %s add column if not exists c varchar", fullTableName1),
          "This connector does not support adding columns");
    } else {
      assertUpdate(
          String.format("alter table %s add column if not exists c varchar", fullTableName1));
      assertThat((String) computeScalar("show create table " + fullTableName1))
          .contains("c varchar");
    }

    assertQueryFails(
        String.format("alter table %s drop column a", fullTableName1),
        "This connector does not support dropping columns");

    // test rename column, but the memory connector is not supported these operations.
    if (trinoVersion < SPI_VERSION_TEST_SUPPORT_RENAME_COLUMN) {
      assertQueryFails(
          String.format("alter table %s rename column b to d ", fullTableName1),
          "This connector does not support renaming columns");
    } else {
      assertUpdate(String.format("alter table %s rename column b to d ", fullTableName1));
      assertThat((String) computeScalar("show create table " + fullTableName1))
          .contains("d integer");
    }

    assertQueryFails(
        String.format("alter table %s alter column a set DATA TYPE int", fullTableName1),
        "This connector does not support setting column types");

    // test set table properties, but the memory connector is not supported these operations.
    assertQueryFails(
        String.format("alter table %s set properties \"max_ttl\" = 20", fullTableName1),
        "This connector does not support setting table properties");

    dropTestTable(fullTableName1);
  }

  @Test
  public void testCreateCatalog() throws Exception {
    // testing the catalogs
    assertThat(computeActual("show catalogs").getOnlyColumnAsSet()).contains("gravitino");
    assertThat(computeActual("show catalogs").getOnlyColumnAsSet()).contains("memory");

    // testing the gravitino connector framework works.
    assertThat(computeActual("select * from system.jdbc.tables").getRowCount()).isGreaterThan(1);

    // test metalake named test. the connector name is gravitino
    assertUpdate("call gravitino.system.create_catalog('memory1', 'memory', Map())");
    assertThat(computeActual("show catalogs").getOnlyColumnAsSet()).contains("memory1");
    assertUpdate("call gravitino.system.drop_catalog('memory1')");
    assertThat(computeActual("show catalogs").getOnlyColumnAsSet()).doesNotContain("memory1");

    // test create catalog with config by trino.bypass.
    assertUpdate(
        "call gravitino.system.create_catalog('memory1', 'memory', Map(array['trino.bypass.memory.max-data-per-node'], array['128MB']))");
    assertThat(computeActual("show catalogs").getOnlyColumnAsSet()).contains("memory1");
    assertUpdate("call gravitino.system.drop_catalog('memory1')");
    assertThat(computeActual("show catalogs").getOnlyColumnAsSet()).doesNotContain("memory1");

    // test create catalog with invalid config by trino.bypass.
    assertQueryFails(
        "call gravitino.system.create_catalog("
            + "catalog=>'memory1', provider=>'memory', properties => Map(array['trino.bypass.unknown-direct-key'], array['10']))",
        // The message must carry the reason the registration failed, not just the fact that it
        // did. (?s) lets .* span the newlines of the underlying configuration error.
        "(?s)Create catalog failed. Create catalog failed due to the loading process fails\\."
            + ".*unknown-direct-key.*");
    assertThat(computeActual("show catalogs").getOnlyColumnAsSet()).doesNotContain("memory1");

    assertUpdate(
        "call gravitino.system.create_catalog("
            + "catalog=>'memory1', provider=>'memory', properties => Map(array['max_ttl'], array['10']), ignore_exist => true)");
    assertThat(computeActual("show catalogs").getOnlyColumnAsSet()).contains("memory1");

    assertUpdate(
        "call gravitino.system.drop_catalog(catalog => 'memory1', ignore_not_exist => true)");
    assertThat(computeActual("show catalogs").getOnlyColumnAsSet()).doesNotContain("memory1");
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

  @Test
  public void testCatalogStatusSystemTable() throws Exception {
    MaterializedResult result =
        computeActual(
            "select metalake, catalog_name, trino_catalog_name, provider, status, last_error,"
                + " failure_count from gravitino.system.catalog_status");
    assertEquals(result.getRowCount(), 1);
    MaterializedRow row = result.getMaterializedRows().get(0);
    assertEquals(row.getField(1), "memory");
    assertEquals(row.getField(2), "memory");
    assertEquals(row.getField(3), "memory");
    assertEquals(row.getField(4), "REGISTERED");
    assertNull(row.getField(5));
    assertEquals(row.getField(6), 0L);
  }

  @Test
  public void testCatalogStatusSystemTableWithReorderedColumns() throws Exception {
    // page.getColumns() honors the requested order; a projection that sorted or de-duplicated
    // channels would return correct looking data in the wrong columns.
    MaterializedResult result =
        computeActual("select status, catalog_name, status from gravitino.system.catalog_status");
    assertEquals(result.getRowCount(), 1);
    MaterializedRow row = result.getMaterializedRows().get(0);
    assertEquals(row.getField(0), "REGISTERED");
    assertEquals(row.getField(1), "memory");
    assertEquals(row.getField(2), "REGISTERED");
  }

  @Test
  public void testStatusSystemTablesWithNoProjectedColumns() throws Exception {
    // count(*) projects no column at all, so the page is built with zero channels and only its
    // position count carries the answer. A table that sized its page from the projected columns
    // would report no rows here while returning them for every other query.
    assertEquals(computeScalar("select count(*) from gravitino.system.catalog_status"), 1L);
    assertEquals(computeScalar("select count(*) from gravitino.system.load_status"), 1L);
    assertEquals(computeScalar("select count(*) from gravitino.system.catalog"), 1L);
  }

  @Test
  public void testCatalogStatusReportsARegistrationFailure() throws Exception {
    // An unknown bypass key makes the inner connector reject its configuration, so the catalog
    // exists in Gravitino but its CREATE CATALOG in Trino fails: a real registration failure
    // rather than a simulated state.
    assertQueryFails(
        "call gravitino.system.create_catalog("
            + "catalog=>'memory_failed', provider=>'memory',"
            + " properties => Map(array['trino.bypass.unknown-direct-key'], array['10']))",
        "(?s)Create catalog failed. Create catalog failed due to the loading process fails\\..*");
    assertThat(computeActual("show catalogs").getOnlyColumnAsSet()).doesNotContain("memory_failed");

    MaterializedResult result =
        computeActual(
            "select status, last_error, failure_count from gravitino.system.catalog_status"
                + " where catalog_name = 'memory_failed'");
    assertEquals(result.getRowCount(), 1);
    MaterializedRow row = result.getMaterializedRows().get(0);
    assertEquals(row.getField(0), "FAILED");
    assertThat((String) row.getField(1)).contains("unknown-direct-key");
    assertEquals(row.getField(2), 1L);

    // Leave the shared query runner as it was found, or the load loop keeps retrying this catalog
    // and the other status table tests see an extra row.
    assertUpdate(
        "call gravitino.system.drop_catalog(catalog => 'memory_failed', ignore_not_exist => true)");
    assertEquals(
        computeActual(
                "select 1 from gravitino.system.catalog_status"
                    + " where catalog_name = 'memory_failed'")
            .getRowCount(),
        0);
  }

  @Test
  public void testLoadStatusSystemTable() throws Exception {
    MaterializedResult result =
        computeActual(
            "select trino_reachable, consecutive_failures, last_error, metalake_errors"
                + " from gravitino.system.load_status");
    assertEquals(result.getRowCount(), 1);
    MaterializedRow row = result.getMaterializedRows().get(0);
    assertEquals(row.getField(0), true);
    assertEquals(row.getField(1), 0L);
    assertNull(row.getField(2));
    assertNull(row.getField(3));
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
