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
package org.apache.gravitino.authorization.ranger.integration.test;

import static org.apache.gravitino.Catalog.AUTHORIZATION_PROVIDER;
import static org.apache.gravitino.connector.AuthorizationPropertiesMeta.RANGER_ADMIN_URL;
import static org.apache.gravitino.connector.AuthorizationPropertiesMeta.RANGER_AUTH_TYPE;
import static org.apache.gravitino.connector.AuthorizationPropertiesMeta.RANGER_PASSWORD;
import static org.apache.gravitino.connector.AuthorizationPropertiesMeta.RANGER_SERVICE_NAME;
import static org.apache.gravitino.connector.AuthorizationPropertiesMeta.RANGER_USERNAME;
import static org.apache.gravitino.integration.test.container.RangerContainer.RANGER_SERVER_PORT;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Configs;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Schema;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.Role;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.authorization.ranger.RangerAuthorizationHivePlugin;
import org.apache.gravitino.authorization.ranger.RangerAuthorizationPlugin;
import org.apache.gravitino.catalog.hive.HiveConstants;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.connector.AuthorizationPropertiesMeta;
import org.apache.gravitino.integration.test.container.HiveContainer;
import org.apache.gravitino.integration.test.container.RangerContainer;
import org.apache.gravitino.integration.test.util.AbstractIT;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.distributions.Strategy;
import org.apache.gravitino.rel.expressions.sorts.NullOrdering;
import org.apache.gravitino.rel.expressions.sorts.SortDirection;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.sorts.SortOrders;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("gravitino-docker-test")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class RangerHiveE2EIT extends AbstractIT {
  private static final Logger LOG = LoggerFactory.getLogger(RangerHiveE2EIT.class);

  private static RangerAuthorizationPlugin rangerAuthPlugin;
  public static final String metalakeName =
      GravitinoITUtils.genRandomName("RangerHiveAuthIT_metalake").toLowerCase();
  public static final String catalogName =
      GravitinoITUtils.genRandomName("RangerHiveAuthIT_catalog").toLowerCase();
  public static final String schemaName =
      GravitinoITUtils.genRandomName("RangerHiveAuthIT_schema").toLowerCase();
  public static final String tableName =
      GravitinoITUtils.genRandomName("RangerHiveAuthIT_table").toLowerCase();

  public static final String HIVE_COL_NAME1 = "hive_col_name1";
  public static final String HIVE_COL_NAME2 = "hive_col_name2";
  public static final String HIVE_COL_NAME3 = "hive_col_name3";

  private static GravitinoMetalake metalake;
  private static Catalog catalog;
  private static final String provider = "hive";
  private static String HIVE_METASTORE_URIS;

  @BeforeAll
  public void startIntegrationTest() throws Exception {
    Map<String, String> configs = Maps.newHashMap();
    configs.put(Configs.ENABLE_AUTHORIZATION.getKey(), String.valueOf(true));
    configs.put(Configs.SERVICE_ADMINS.getKey(), AuthConstants.ANONYMOUS_USER);
    registerCustomConfigs(configs);
    super.startIntegrationTest();

    RangerITEnv.setup();
    containerSuite.startHiveContainer();
    HIVE_METASTORE_URIS =
        String.format(
            "thrift://%s:%d",
            containerSuite.getHiveContainer().getContainerIpAddress(),
            HiveContainer.HIVE_METASTORE_PORT);

    createMetalake();
    createCatalogAndRangerAuthPlugin();
    createSchema();
    createHiveTable();
  }

  @AfterAll
  public void stop() throws IOException {
    client = null;
  }

  @Test
  void testCreateRole() {
    String roleName = RangerITEnv.currentFunName();
    Map<String, String> properties = Maps.newHashMap();
    properties.put("k1", "v1");

    SecurableObject table1 =
        SecurableObjects.parse(
            String.format("%s.%s.%s", catalogName, schemaName, tableName),
            MetadataObject.Type.TABLE,
            Lists.newArrayList(Privileges.SelectTable.allow()));
    Role role = metalake.createRole(roleName, properties, Lists.newArrayList(table1));
    RangerITEnv.verifyRoleInRanger(rangerAuthPlugin, role);
  }

  private void createMetalake() {
    GravitinoMetalake[] gravitinoMetalakes = client.listMetalakes();
    Assertions.assertEquals(0, gravitinoMetalakes.length);

    GravitinoMetalake createdMetalake =
        client.createMetalake(metalakeName, "comment", Collections.emptyMap());
    GravitinoMetalake loadMetalake = client.loadMetalake(metalakeName);
    Assertions.assertEquals(createdMetalake, loadMetalake);

    metalake = loadMetalake;
  }

  private static void createCatalogAndRangerAuthPlugin() {
    rangerAuthPlugin =
        RangerAuthorizationHivePlugin.getInstance(
            ImmutableMap.of(
                AuthorizationPropertiesMeta.RANGER_ADMIN_URL,
                String.format(
                    "http://%s:%d",
                    containerSuite.getRangerContainer().getContainerIpAddress(),
                    RangerContainer.RANGER_SERVER_PORT),
                AuthorizationPropertiesMeta.RANGER_AUTH_TYPE,
                RangerContainer.authType,
                AuthorizationPropertiesMeta.RANGER_USERNAME,
                RangerContainer.rangerUserName,
                AuthorizationPropertiesMeta.RANGER_PASSWORD,
                RangerContainer.rangerPassword,
                AuthorizationPropertiesMeta.RANGER_SERVICE_NAME,
                RangerITEnv.RANGER_HIVE_REPO_NAME));

    Map<String, String> properties = Maps.newHashMap();
    properties.put(HiveConstants.METASTORE_URIS, HIVE_METASTORE_URIS);
    properties.put(AUTHORIZATION_PROVIDER, "ranger");
    properties.put(RANGER_SERVICE_NAME, RangerITEnv.RANGER_HIVE_REPO_NAME);
    properties.put(
        RANGER_ADMIN_URL,
        String.format(
            "http://localhost:%s",
            containerSuite.getRangerContainer().getMappedPort(RANGER_SERVER_PORT)));
    properties.put(RANGER_AUTH_TYPE, RangerContainer.authType);
    properties.put(RANGER_USERNAME, RangerContainer.rangerUserName);
    properties.put(RANGER_PASSWORD, RangerContainer.rangerPassword);

    metalake.createCatalog(catalogName, Catalog.Type.RELATIONAL, provider, "comment", properties);
    catalog = metalake.loadCatalog(catalogName);
    LOG.info("Catalog created: {}", catalog);
  }

  private static void createSchema() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put("key1", "val1");
    properties.put("key2", "val2");
    properties.put(
        "location",
        String.format(
            "hdfs://%s:%d/user/hive/warehouse/%s.db",
            containerSuite.getHiveContainer().getContainerIpAddress(),
            HiveContainer.HDFS_DEFAULTFS_PORT,
            schemaName.toLowerCase()));
    String comment = "comment";

    catalog.asSchemas().createSchema(schemaName, comment, properties);
    Schema loadSchema = catalog.asSchemas().loadSchema(schemaName);
    Assertions.assertEquals(schemaName.toLowerCase(), loadSchema.name());
  }

  public static void createHiveTable() {
    // Create table from Gravitino API
    Column[] columns = createColumns();
    NameIdentifier nameIdentifier = NameIdentifier.of(schemaName, tableName);

    Distribution distribution =
        Distributions.of(Strategy.EVEN, 10, NamedReference.field(HIVE_COL_NAME1));

    final SortOrder[] sortOrders =
        new SortOrder[] {
          SortOrders.of(
              NamedReference.field(HIVE_COL_NAME2),
              SortDirection.DESCENDING,
              NullOrdering.NULLS_FIRST)
        };

    Map<String, String> properties = ImmutableMap.of("key1", "val1", "key2", "val2");
    Table createdTable =
        catalog
            .asTableCatalog()
            .createTable(
                nameIdentifier,
                columns,
                "table_comment",
                properties,
                Transforms.EMPTY_TRANSFORM,
                distribution,
                sortOrders);
    LOG.info("Table created: {}", createdTable);
  }

  private static Column[] createColumns() {
    Column col1 = Column.of(HIVE_COL_NAME1, Types.ByteType.get(), "col_1_comment");
    Column col2 = Column.of(HIVE_COL_NAME2, Types.DateType.get(), "col_2_comment");
    Column col3 = Column.of(HIVE_COL_NAME3, Types.StringType.get(), "col_3_comment");
    return new Column[] {col1, col2, col3};
  }
}
