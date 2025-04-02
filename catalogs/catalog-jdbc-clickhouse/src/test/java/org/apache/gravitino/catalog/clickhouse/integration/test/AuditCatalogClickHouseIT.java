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

package org.apache.gravitino.catalog.clickhouse.integration.test;

import static org.apache.gravitino.catalog.clickhouse.converter.ClickHouseUtils.getSortOrders;
import static org.apache.gravitino.rel.Column.DEFAULT_VALUE_NOT_SET;

import com.google.common.collect.Maps;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.CatalogChange;
import org.apache.gravitino.Configs;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Schema;
import org.apache.gravitino.auth.AuthenticatorType;
import org.apache.gravitino.catalog.clickhouse.integration.test.service.ClickHouseService;
import org.apache.gravitino.catalog.jdbc.config.JdbcConfig;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.integration.test.container.ClickHouseContainer;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.integration.test.util.TestDatabaseName;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("gravitino-docker-test")
public class AuditCatalogClickHouseIT extends BaseIT {
  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();
  public static final String metalakeName =
      GravitinoITUtils.genRandomName("audit_clickhouse_metalake");
  private static final String expectUser = System.getProperty("user.name");
  protected static TestDatabaseName TEST_DB_NAME;
  private static final String provider = "jdbc-clickhouse";

  private static ClickHouseService clickhouseService;
  private static ClickHouseContainer CLICKHOUSE_CONTAINER;
  private static GravitinoMetalake metalake;

  @BeforeAll
  public void startIntegrationTest() throws Exception {
    Map<String, String> configs = Maps.newHashMap();
    configs.put(Configs.AUTHENTICATORS.getKey(), AuthenticatorType.SIMPLE.name().toLowerCase());
    registerCustomConfigs(configs);
    super.startIntegrationTest();

    containerSuite.startClickHouseContainer(
        TestDatabaseName.CLICKHOUSE_AUDIT_CATALOG_CLICKHOUSE_IT);
    CLICKHOUSE_CONTAINER = containerSuite.getClickHouseContainer();
    TEST_DB_NAME = TestDatabaseName.CLICKHOUSE_AUDIT_CATALOG_CLICKHOUSE_IT;
    clickhouseService =
        new ClickHouseService(containerSuite.getClickHouseContainer(), TEST_DB_NAME);
    createMetalake();
  }

  @AfterAll
  public void stopIntegrationTest() throws IOException, InterruptedException {
    client.dropMetalake(metalakeName, true);
    clickhouseService.close();
    super.stopIntegrationTest();
  }

  @Test
  public void testAuditCatalog() throws Exception {
    String catalogName = GravitinoITUtils.genRandomName("audit_clickhouse_catalog");
    Catalog catalog = createCatalog(catalogName);

    Assertions.assertEquals(expectUser, catalog.auditInfo().creator());
    Assertions.assertEquals(catalog.auditInfo().creator(), catalog.auditInfo().lastModifier());
    Assertions.assertEquals(
        catalog.auditInfo().createTime(), catalog.auditInfo().lastModifiedTime());
    catalog = metalake.alterCatalog(catalogName, CatalogChange.setProperty("key1", "value1"));
    Assertions.assertEquals(expectUser, catalog.auditInfo().creator());
    Assertions.assertEquals(expectUser, catalog.auditInfo().lastModifier());

    metalake.dropCatalog(catalogName, true);
  }

  @Test
  public void testAuditSchema() throws Exception {
    String catalogName = GravitinoITUtils.genRandomName("audit_clickhouse_schema_catalog");
    String schemaName = GravitinoITUtils.genRandomName("audit_clickhouse_schema");
    Catalog catalog = createCatalog(catalogName);
    Map<String, String> prop = Maps.newHashMap();
    Schema schema = catalog.asSchemas().createSchema(schemaName, null, prop);
    Assertions.assertEquals(expectUser, schema.auditInfo().creator());
    Assertions.assertNull(schema.auditInfo().lastModifier());

    catalog.asSchemas().dropSchema(schemaName, true);
    metalake.dropCatalog(catalogName, true);
  }

  @Test
  public void testAuditTable() throws Exception {
    String catalogName = GravitinoITUtils.genRandomName("audit_clickhouse_table_catalog");
    String schemaName = GravitinoITUtils.genRandomName("audit_clickhouse_table_schema");
    String tableName = GravitinoITUtils.genRandomName("audit_clickhouse_table");
    Catalog catalog = createCatalog(catalogName);
    Map<String, String> properties = Maps.newHashMap();

    Column col1 =
        Column.of(
            "col_1", Types.IntegerType.get(), "col_1_comment", false, false, DEFAULT_VALUE_NOT_SET);

    catalog.asSchemas().createSchema(schemaName, null, properties);
    Table table =
        catalog
            .asTableCatalog()
            .createTable(
                NameIdentifier.of(schemaName, tableName),
                new Column[] {col1},
                "comment",
                properties,
                null,
                null,
                getSortOrders("col_1"));
    Assertions.assertEquals(expectUser, table.auditInfo().creator());
    Assertions.assertNull(table.auditInfo().lastModifier());
    table =
        catalog
            .asTableCatalog()
            .alterTable(
                NameIdentifier.of(schemaName, tableName),
                TableChange.addColumn(new String[] {"col_4"}, Types.StringType.get()));
    Assertions.assertEquals(expectUser, table.auditInfo().creator());
    Assertions.assertEquals(expectUser, table.auditInfo().lastModifier());

    catalog.asTableCatalog().dropTable(NameIdentifier.of(schemaName, tableName));
    catalog.asSchemas().dropSchema(schemaName, true);
    metalake.dropCatalog(catalogName, true);
  }

  private static Catalog createCatalog(String catalogName) throws SQLException {
    Map<String, String> catalogProperties = Maps.newHashMap();

    catalogProperties.put(
        JdbcConfig.JDBC_URL.getKey(),
        StringUtils.substring(
            CLICKHOUSE_CONTAINER.getJdbcUrl(TEST_DB_NAME),
            0,
            CLICKHOUSE_CONTAINER.getJdbcUrl(TEST_DB_NAME).lastIndexOf("/")));
    catalogProperties.put(
        JdbcConfig.JDBC_DRIVER.getKey(), CLICKHOUSE_CONTAINER.getDriverClassName(TEST_DB_NAME));
    catalogProperties.put(JdbcConfig.USERNAME.getKey(), CLICKHOUSE_CONTAINER.getUsername());
    catalogProperties.put(JdbcConfig.PASSWORD.getKey(), CLICKHOUSE_CONTAINER.getPassword());

    return metalake.createCatalog(
        catalogName, Catalog.Type.RELATIONAL, provider, "comment", catalogProperties);
  }

  private void createMetalake() {
    GravitinoMetalake[] gravitinoMetalakes = client.listMetalakes();
    Assertions.assertEquals(0, gravitinoMetalakes.length);

    client.createMetalake(metalakeName, "comment", Collections.emptyMap());
    GravitinoMetalake loadMetalake = client.loadMetalake(metalakeName);
    Assertions.assertEquals(metalakeName, loadMetalake.name());
    metalake = loadMetalake;
  }
}
