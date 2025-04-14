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
import static org.apache.gravitino.catalog.hive.HiveConstants.IMPERSONATION_ENABLE;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Configs;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.auth.AuthenticatorType;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.authorization.common.RangerAuthorizationProperties;
import org.apache.gravitino.catalog.hive.HiveConstants;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.exceptions.UserAlreadyExistsException;
import org.apache.gravitino.integration.test.container.HiveContainer;
import org.apache.gravitino.integration.test.container.RangerContainer;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.types.Types;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("gravitino-docker-test")
public class RangerHiveHdfsE2EIT extends BaseIT {

  private static final Logger LOG = LoggerFactory.getLogger(RangerHiveHdfsE2EIT.class);
  private static final String provider = "hive";
  private static final String HADOOP_USER_NAME = "HADOOP_USER_NAME";
  public static final String HIVE_COL_NAME1 = "hive_col_name1";
  public static final String HIVE_COL_NAME2 = "hive_col_name2";
  public static final String HIVE_COL_NAME3 = "hive_col_name3";
  private static String metalakeName;
  private static GravitinoMetalake metalake;
  private static String catalogName;
  private static String HIVE_METASTORE_URIS;
  private static Catalog catalog;

  private static String DEFAULT_FS;

  @BeforeAll
  public void startIntegrationTest() throws Exception {
    metalakeName = GravitinoITUtils.genRandomName("metalake").toLowerCase();
    catalogName = GravitinoITUtils.genRandomName("catalog").toLowerCase();

    // Enable Gravitino Authorization mode
    Map<String, String> configs = Maps.newHashMap();
    configs.put(Configs.ENABLE_AUTHORIZATION.getKey(), String.valueOf(true));
    configs.put(Configs.SERVICE_ADMINS.getKey(), RangerITEnv.HADOOP_USER_NAME);
    configs.put(Configs.AUTHENTICATORS.getKey(), AuthenticatorType.SIMPLE.name().toLowerCase());
    configs.put("SimpleAuthUserName", AuthConstants.ANONYMOUS_USER);
    registerCustomConfigs(configs);

    super.startIntegrationTest();
    RangerITEnv.init(RangerBaseE2EIT.metalakeName, false);
    RangerITEnv.startHiveRangerContainer();

    DEFAULT_FS =
        String.format(
            "hdfs://%s:%d/user/hive/warehouse",
            containerSuite.getHiveRangerContainer().getContainerIpAddress(),
            HiveContainer.HDFS_DEFAULTFS_PORT);

    HIVE_METASTORE_URIS =
        String.format(
            "thrift://%s:%d",
            containerSuite.getHiveRangerContainer().getContainerIpAddress(),
            HiveContainer.HIVE_METASTORE_PORT);

    createMetalake();
    createCatalog();

    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", DEFAULT_FS);

    RangerITEnv.cleanup();
    try {
      metalake.addUser(System.getenv(HADOOP_USER_NAME));
    } catch (UserAlreadyExistsException e) {
      LOG.error("Failed to add user: {}", System.getenv(HADOOP_USER_NAME), e);
    }
  }

  @AfterAll
  void cleanIT() {
    if (client != null) {
      Arrays.stream(catalog.asSchemas().listSchemas())
          .filter(schema -> !schema.equals("default"))
          .forEach(
              (schema -> {
                catalog.asSchemas().dropSchema(schema, false);
              }));

      // The `dropCatalog` call will invoke the catalog metadata object to remove privileges
      Arrays.stream(metalake.listCatalogs())
          .forEach((catalogName -> metalake.dropCatalog(catalogName, true)));
      client.disableMetalake(metalakeName);
      client.dropMetalake(metalakeName);
    }

    try {
      closer.close();
    } catch (Exception e) {
      LOG.error("Failed to close CloseableGroup", e);
    }
    client = null;
    RangerITEnv.cleanup();
  }

  protected void createCatalog() {
    Map<String, String> properties =
        ImmutableMap.of(
            HiveConstants.METASTORE_URIS,
            HIVE_METASTORE_URIS,
            IMPERSONATION_ENABLE,
            "true",
            AUTHORIZATION_PROVIDER,
            "ranger",
            RangerAuthorizationProperties.RANGER_SERVICE_TYPE,
            "HDFS",
            RangerAuthorizationProperties.RANGER_SERVICE_NAME,
            RangerITEnv.RANGER_HDFS_REPO_NAME,
            RangerAuthorizationProperties.RANGER_ADMIN_URL,
            RangerITEnv.RANGER_ADMIN_URL,
            RangerAuthorizationProperties.RANGER_AUTH_TYPE,
            RangerContainer.authType,
            RangerAuthorizationProperties.RANGER_USERNAME,
            RangerContainer.rangerUserName,
            RangerAuthorizationProperties.RANGER_PASSWORD,
            RangerContainer.rangerPassword);

    metalake.createCatalog(catalogName, Catalog.Type.RELATIONAL, provider, "comment", properties);
    catalog = metalake.loadCatalog(catalogName);
    LOG.info("Catalog created: {}", catalog);
  }

  private void createMetalake() {
    GravitinoMetalake[] gravitinoMetalakes = client.listMetalakes();
    Assertions.assertEquals(0, gravitinoMetalakes.length);

    client.createMetalake(metalakeName, "comment", Collections.emptyMap());
    GravitinoMetalake loadMetalake = client.loadMetalake(metalakeName);
    Assertions.assertEquals(metalakeName, loadMetalake.name());

    metalake = loadMetalake;
  }

  @Test
  public void testRenameTable() throws Exception {
    // 1. Create a table
    TableCatalog tableCatalog = catalog.asTableCatalog();
    tableCatalog.createTable(
        NameIdentifier.of("default", "test"), createColumns(), "comment1", ImmutableMap.of());

    // 2. check the privileges, should throw an exception
    String userName = "test";
    metalake.addUser(userName);
    UserGroupInformation.createProxyUser(userName, UserGroupInformation.getCurrentUser())
        .doAs(
            (PrivilegedExceptionAction<Void>)
                () -> {
                  Configuration conf = new Configuration();
                  conf.set("fs.defaultFS", DEFAULT_FS);
                  String path = String.format("%s/test", DEFAULT_FS);
                  FileSystem userFileSystem = FileSystem.get(conf);
                  Exception e =
                      Assertions.assertThrows(
                          Exception.class,
                          () ->
                              userFileSystem.mkdirs(
                                  new Path(String.format("%s/%s", path, "test1"))));
                  Assertions.assertTrue(e.getMessage().contains("Permission denied"));
                  userFileSystem.close();
                  return null;
                });

    // 3. Grant the privileges to the table
    SecurableObject catalogObject =
        SecurableObjects.ofCatalog(catalogName, Collections.emptyList());
    SecurableObject schemaObject =
        SecurableObjects.ofSchema(catalogObject, "default", Collections.emptyList());
    SecurableObject tableObject =
        SecurableObjects.ofTable(
            schemaObject, "test", Lists.newArrayList(Privileges.ModifyTable.allow()));
    metalake.createRole(
        "hdfs_rename_role", Collections.emptyMap(), Lists.newArrayList(tableObject));

    metalake.grantRolesToUser(Lists.newArrayList("hdfs_rename_role"), userName);
    RangerBaseE2EIT.waitForUpdatingPolicies();

    UserGroupInformation.createProxyUser(userName, UserGroupInformation.getCurrentUser())
        .doAs(
            (PrivilegedExceptionAction<Void>)
                () -> {
                  Configuration conf = new Configuration();
                  conf.set("fs.defaultFS", DEFAULT_FS);
                  String path = String.format("%s/test", DEFAULT_FS);
                  FileSystem userFileSystem = FileSystem.get(conf);
                  Assertions.assertDoesNotThrow(() -> userFileSystem.listStatus(new Path(path)));
                  Assertions.assertDoesNotThrow(
                      () -> userFileSystem.mkdirs(new Path(String.format("%s/%s", path, "test1"))));
                  userFileSystem.close();
                  return null;
                });
    // 4. Rename the table
    tableCatalog.alterTable(NameIdentifier.of("default", "test"), TableChange.rename("test1"));

    // 5. Check the privileges
    UserGroupInformation.createProxyUser(userName, UserGroupInformation.getCurrentUser())
        .doAs(
            (PrivilegedExceptionAction<Void>)
                () -> {
                  Configuration conf = new Configuration();
                  conf.set("fs.defaultFS", DEFAULT_FS);
                  String path = String.format("%s/test1", DEFAULT_FS);
                  FileSystem userFileSystem = FileSystem.get(conf);
                  Assertions.assertDoesNotThrow(() -> userFileSystem.listStatus(new Path(path)));
                  Assertions.assertDoesNotThrow(
                      () -> userFileSystem.mkdirs(new Path(String.format("%s/%s", path, "test1"))));
                  userFileSystem.close();
                  return null;
                });
  }

  private Column[] createColumns() {
    Column col1 = Column.of(HIVE_COL_NAME1, Types.ByteType.get(), "col_1_comment");
    Column col2 = Column.of(HIVE_COL_NAME2, Types.DateType.get(), "col_2_comment");
    Column col3 = Column.of(HIVE_COL_NAME3, Types.StringType.get(), "col_3_comment");
    return new Column[] {col1, col2, col3};
  }
}
