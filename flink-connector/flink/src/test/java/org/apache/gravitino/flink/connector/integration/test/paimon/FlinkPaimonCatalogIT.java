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
package org.apache.gravitino.flink.connector.integration.test.paimon;

import static org.apache.gravitino.flink.connector.integration.test.utils.TestUtils.toFlinkPhysicalColumn;
import static org.junit.jupiter.api.Assertions.fail;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.types.Row;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.flink.connector.integration.test.FlinkCommonIT;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.distributions.Strategy;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public abstract class FlinkPaimonCatalogIT extends FlinkCommonIT {

  protected org.apache.gravitino.Catalog catalog;

  @Override
  protected boolean supportSchemaOperationWithCommentAndOptions() {
    return false;
  }

  @Override
  protected String getProvider() {
    return "lakehouse-paimon";
  }

  @Override
  protected boolean supportDropCascade() {
    return true;
  }

  protected Catalog currentCatalog() {
    return catalog;
  }

  private void initPaimonCatalog() {
    Preconditions.checkArgument(metalake != null, "metalake should not be null");
    catalog =
        metalake.createCatalog(
            getPaimonCatalogName(),
            org.apache.gravitino.Catalog.Type.RELATIONAL,
            getProvider(),
            null,
            getPaimonCatalogOptions());
  }

  protected abstract void createGravitinoCatalogByFlinkSql(String catalogName);

  protected abstract String getPaimonCatalogName();

  protected abstract Map<String, String> getPaimonCatalogOptions();

  @BeforeAll
  void paimonSetup() {
    initPaimonCatalog();
  }

  @AfterAll
  void paimonStop() {
    Preconditions.checkArgument(metalake != null, "metalake should not be null");
    metalake.dropCatalog(getPaimonCatalogName(), true);
  }

  protected abstract String getWarehouse();

  @Test
  public void testBucketDistributionRoundTrip() {
    String databaseName = "test_bucket_distribution_db";
    String tableName = "test_bucket_table";

    doWithSchema(
        currentCatalog(),
        databaseName,
        catalog -> {
          sql(
              "CREATE TABLE %s (id BIGINT, name STRING) "
                  + "WITH ('bucket' = '4', 'bucket-key' = 'id')",
              tableName);

          Table table =
              catalog.asTableCatalog().loadTable(NameIdentifier.of(databaseName, tableName));
          Assertions.assertEquals(Strategy.HASH, table.distribution().strategy());
          Assertions.assertEquals(4, table.distribution().number());
          Assertions.assertEquals(1, table.distribution().expressions().length);
          Assertions.assertEquals("id", table.distribution().expressions()[0].toString());

          TableResult showResult = sql("SHOW CREATE TABLE %s", tableName);
          List<Row> rows = Lists.newArrayList(showResult.collect());
          Assertions.assertEquals(1, rows.size());
          String createTableDDL = rows.get(0).getField(0).toString();
          Assertions.assertTrue(
              createTableDDL.contains("'bucket' = '4'"),
              "SHOW CREATE TABLE should contain bucket number, but was: " + createTableDDL);
          Assertions.assertTrue(
              createTableDDL.contains("'bucket-key' = 'id'"),
              "SHOW CREATE TABLE should contain bucket-key, but was: " + createTableDDL);
        },
        true,
        supportDropCascade());
  }

  @Test
  public void testDynamicBucketDistributionRoundTrip() {
    String databaseName = "test_dynamic_bucket_db";
    String tableName = "test_dynamic_bucket_table";

    doWithSchema(
        currentCatalog(),
        databaseName,
        catalog -> {
          sql(
              "CREATE TABLE %s (id BIGINT, name STRING, PRIMARY KEY (id) NOT ENFORCED) "
                  + "WITH ('bucket' = '-1')",
              tableName);

          Table table =
              catalog.asTableCatalog().loadTable(NameIdentifier.of(databaseName, tableName));
          Assertions.assertEquals(Strategy.HASH, table.distribution().strategy());
          Assertions.assertEquals(Distributions.AUTO, table.distribution().number());
          Assertions.assertEquals(0, table.distribution().expressions().length);
        },
        true,
        supportDropCascade());
  }

  @Test
  public void testBucketKeyWithDynamicBucketNumRejected() {
    String databaseName = "test_bucket_key_dynamic_rejected_db";
    String tableName = "test_rejected_table";

    // Dynamic bucket mode ('-1') does not accept bucket-ley statement
    // ref:
    // https://github.com/apache/paimon/blob/dd2273f70d2f5298a3a35a557c6b462f961e3647/paimon-core/src/main/java/org/apache/paimon/schema/SchemaValidation.java#L568-L572
    doWithSchema(
        currentCatalog(),
        databaseName,
        catalog -> {
          Exception exception =
              Assertions.assertThrows(
                  Exception.class,
                  () ->
                      sql(
                          "CREATE TABLE %s (id BIGINT, name STRING) "
                              + "WITH ('bucket' = '-1', 'bucket-key' = 'id')",
                          tableName));
          Assertions.assertTrue(
              exception.getMessage().contains("bucket-key")
                  || exception.getCause().getMessage().contains("bucket-key"),
              "Error should mention bucket-key, but was: " + exception.getMessage());
        },
        true,
        supportDropCascade());
  }

  @Test
  public void testOnlyBucketKeyWithoutNoBucketNumRejected() {
    String databaseName = "test_only_bucket_key_rejected_db";
    String tableName = "test_rejected_table";

    doWithSchema(
        currentCatalog(),
        databaseName,
        catalog -> {
          // Only bucket-key without bucket maps to auto(HASH, [id]) which sets
          // bucket=-1 and bucket-key=id in Paimon options. Paimon rejects this.
          // ref:
          // https://github.com/apache/paimon/blob/dd2273f70d2f5298a3a35a557c6b462f961e3647/paimon-core/src/main/java/org/apache/paimon/schema/SchemaValidation.java#L568-L572
          Exception exception =
              Assertions.assertThrows(
                  Exception.class,
                  () ->
                      sql(
                          "CREATE TABLE %s (id BIGINT, name STRING) "
                              + "WITH ('bucket-key' = 'id')",
                          tableName));
          Assertions.assertTrue(
              exception.getMessage().contains("bucket-key")
                  || exception.getCause().getMessage().contains("bucket-key"),
              "Error should mention bucket-key, but was: " + exception.getMessage());
        },
        true,
        supportDropCascade());
  }

  @Test
  public void testCreateGravitinoPaimonCatalogUsingSQL() {
    tableEnv.useCatalog(DEFAULT_CATALOG);
    int numCatalogs = tableEnv.listCatalogs().length;
    String catalogName = "gravitino_paimon_catalog";
    createGravitinoCatalogByFlinkSql(catalogName);
    String[] catalogs = tableEnv.listCatalogs();
    Assertions.assertEquals(numCatalogs + 1, catalogs.length, "Should create a new catalog");
    Assertions.assertTrue(metalake.catalogExists(catalogName));
    org.apache.gravitino.Catalog gravitinoCatalog = metalake.loadCatalog(catalogName);
    Map<String, String> properties = gravitinoCatalog.properties();
    Assertions.assertEquals(getWarehouse(), properties.get("warehouse"));
    tableEnv.executeSql("drop catalog " + catalogName);
    Assertions.assertFalse(metalake.catalogExists(catalogName));
    Assertions.assertEquals(
        numCatalogs, tableEnv.listCatalogs().length, "The created catalog should be dropped.");
  }

  @Test
  public void testMultisetType() {
    String databaseName = "test_multiset_type_db";
    String tableName = "test_multiset_table";

    doWithSchema(
        currentCatalog(),
        databaseName,
        catalog -> {
          // Create a table with MULTISET column using ExternalType through Gravitino API.
          // This simulates the scenario where a Paimon table with MULTISET type is registered
          // in Gravitino (e.g., created by native Paimon and then managed by Gravitino).
          Column[] columns =
              new Column[] {
                Column.of("id", Types.LongType.get(), "id"),
                Column.of(
                    "field_multiset", Types.ExternalType.of("MULTISET<STRING>"), "multiset field")
              };
          catalog
              .asTableCatalog()
              .createTable(
                  NameIdentifier.of(databaseName, tableName),
                  columns,
                  "test multiset table",
                  ImmutableMap.of());

          // Verify that getTable through Gravitino Flink Connector returns MULTISET type
          Optional<org.apache.flink.table.catalog.Catalog> flinkCatalog =
              tableEnv.getCatalog(catalog.name());
          Assertions.assertTrue(flinkCatalog.isPresent());
          try {
            CatalogBaseTable table =
                flinkCatalog.get().getTable(new ObjectPath(databaseName, tableName));
            Assertions.assertNotNull(table);

            org.apache.flink.table.catalog.Column[] expected =
                new org.apache.flink.table.catalog.Column[] {
                  org.apache.flink.table.catalog.Column.physical("id", DataTypes.BIGINT())
                      .withComment("id"),
                  org.apache.flink.table.catalog.Column.physical(
                          "field_multiset", DataTypes.MULTISET(DataTypes.STRING()))
                      .withComment("multiset field")
                };
            org.apache.flink.table.catalog.Column[] actual =
                toFlinkPhysicalColumn(table.getUnresolvedSchema().getColumns());
            Assertions.assertArrayEquals(expected, actual);
          } catch (TableNotExistException e) {
            fail(e);
          }
        },
        true,
        supportDropCascade());
  }
}
