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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.Map;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.flink.connector.integration.test.FlinkCommonIT;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.distributions.Strategy;
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
          Assertions.assertEquals(1, table.distribution().expressions().length);
          Assertions.assertEquals("id", table.distribution().expressions()[0].toString());

          TableResult showResult = sql("SHOW CREATE TABLE %s", tableName);
          List<Row> rows = Lists.newArrayList(showResult.collect());
          Assertions.assertEquals(1, rows.size());
          String createTableDDL = rows.get(0).getField(0).toString();
          Assertions.assertTrue(
              createTableDDL.contains("'bucket' = '-1'"),
              "SHOW CREATE TABLE should contain bucket=-1, but was: " + createTableDDL);
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
}
