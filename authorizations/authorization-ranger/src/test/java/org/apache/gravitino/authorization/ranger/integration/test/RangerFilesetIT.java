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
import static org.apache.gravitino.authorization.ranger.integration.test.RangerITEnv.currentFunName;
import static org.apache.gravitino.authorization.ranger.integration.test.RangerITEnv.rangerClient;
import static org.apache.gravitino.authorization.ranger.integration.test.RangerITEnv.rangerHelper;
import static org.apache.gravitino.catalog.hive.HiveConstants.IMPERSONATION_ENABLE;
import static org.apache.gravitino.integration.test.container.RangerContainer.RANGER_SERVER_PORT;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Configs;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Schema;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.auth.AuthenticatorType;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.authorization.common.RangerAuthorizationProperties;
import org.apache.gravitino.authorization.ranger.RangerHelper;
import org.apache.gravitino.authorization.ranger.RangerPrivileges;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.file.FilesetChange;
import org.apache.gravitino.integration.test.container.HiveContainer;
import org.apache.gravitino.integration.test.container.RangerContainer;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ranger.RangerServiceException;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("gravitino-docker-test")
public class RangerFilesetIT extends BaseIT {
  private static final Logger LOG = LoggerFactory.getLogger(RangerFilesetIT.class);

  private String RANGER_ADMIN_URL;
  private String defaultBaseLocation;
  private String metalakeName = "metalake";
  private String catalogName = GravitinoITUtils.genRandomName("RangerFilesetE2EIT_catalog");
  private String schemaName = GravitinoITUtils.genRandomName("RangerFilesetE2EIT_schema");
  private static final String provider = "hadoop";
  private FileSystem fileSystem;
  private GravitinoMetalake metalake;
  private Catalog catalog;

  @BeforeAll
  public void startIntegrationTest() throws Exception {
    // Enable Gravitino Authorization mode
    Map<String, String> configs = Maps.newHashMap();
    configs.put(Configs.ENABLE_AUTHORIZATION.getKey(), String.valueOf(true));
    configs.put(Configs.SERVICE_ADMINS.getKey(), RangerITEnv.HADOOP_USER_NAME);
    configs.put(Configs.AUTHENTICATORS.getKey(), AuthenticatorType.SIMPLE.name().toLowerCase());
    configs.put("SimpleAuthUserName", AuthConstants.ANONYMOUS_USER);
    configs.put(
        Configs.AUTHORIZATION_IMPL.getKey(),
        "org.apache.gravitino.server.authorization.PassThroughAuthorizer");
    registerCustomConfigs(configs);
    super.startIntegrationTest();

    RangerITEnv.init(metalakeName, false);
    RangerITEnv.startHiveRangerContainer();

    RANGER_ADMIN_URL =
        String.format(
            "http://%s:%d",
            containerSuite.getRangerContainer().getContainerIpAddress(), RANGER_SERVER_PORT);

    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", defaultBaseLocation());
    fileSystem = FileSystem.get(conf);

    createCatalogAndSchema();
  }

  @AfterAll
  public void stop() throws IOException {
    if (client != null) {
      Arrays.stream(catalog.asSchemas().listSchemas())
          .filter(schema -> !schema.equals("default"))
          .forEach(
              (schema -> {
                catalog.asSchemas().dropSchema(schema, false);
              }));
      Arrays.stream(metalake.listCatalogs())
          .forEach((catalogName -> metalake.dropCatalog(catalogName, true)));
      client.disableMetalake(metalakeName);
      client.dropMetalake(metalakeName);
    }
    if (fileSystem != null) {
      fileSystem.close();
    }
    try {
      closer.close();
    } catch (Exception e) {
      LOG.error("Failed to close CloseableGroup", e);
    }
    client = null;
    RangerITEnv.cleanup();
  }

  @Test
  @Order(0)
  void testReadWritePath() throws IOException, RangerServiceException {
    String filename = GravitinoITUtils.genRandomName("RangerFilesetE2EIT_fileset");
    Fileset fileset =
        catalog
            .asFilesetCatalog()
            .createFileset(
                NameIdentifier.of(schemaName, filename),
                "comment",
                Fileset.Type.MANAGED,
                storageLocation(filename),
                null);
    Assertions.assertTrue(
        catalog.asFilesetCatalog().filesetExists(NameIdentifier.of(schemaName, fileset.name())));
    Assertions.assertTrue(fileSystem.exists(new Path(storageLocation(filename))));
    List<RangerPolicy> policies =
        rangerClient.getPoliciesInService(RangerITEnv.RANGER_HDFS_REPO_NAME);
    Assertions.assertEquals(2, policies.size());
    Assertions.assertEquals(3, policies.get(0).getPolicyItems().size());

    Assertions.assertEquals(
        1,
        policies.get(0).getPolicyItems().stream()
            .filter(item -> item.getRoles().contains(RangerHelper.GRAVITINO_OWNER_ROLE))
            .filter(
                item ->
                    item.getAccesses().stream()
                        .anyMatch(
                            access ->
                                access
                                    .getType()
                                    .equals(RangerPrivileges.RangerHdfsPrivilege.READ.getName())))
            .count());
    Assertions.assertEquals(
        1,
        policies.get(0).getPolicyItems().stream()
            .filter(item -> item.getRoles().contains(RangerHelper.GRAVITINO_OWNER_ROLE))
            .filter(
                item ->
                    item.getAccesses().stream()
                        .anyMatch(
                            access ->
                                access
                                    .getType()
                                    .equals(RangerPrivileges.RangerHdfsPrivilege.WRITE.getName())))
            .count());
    Assertions.assertEquals(
        1,
        policies.get(0).getPolicyItems().stream()
            .filter(item -> item.getRoles().contains(RangerHelper.GRAVITINO_OWNER_ROLE))
            .filter(
                item ->
                    item.getAccesses().stream()
                        .anyMatch(
                            access ->
                                access
                                    .getType()
                                    .equals(
                                        RangerPrivileges.RangerHdfsPrivilege.EXECUTE.getName())))
            .count());

    String filesetRole = currentFunName();
    SecurableObject securableObject =
        SecurableObjects.parse(
            String.format("%s.%s.%s", catalogName, schemaName, fileset.name()),
            MetadataObject.Type.FILESET,
            Lists.newArrayList(Privileges.ReadFileset.allow()));
    metalake.createRole(filesetRole, Collections.emptyMap(), Lists.newArrayList(securableObject));

    policies = rangerClient.getPoliciesInService(RangerITEnv.RANGER_HDFS_REPO_NAME);
    Assertions.assertEquals(2, policies.size());
    Assertions.assertEquals(3, policies.get(0).getPolicyItems().size());
    Assertions.assertEquals(
        1,
        policies.get(1).getPolicyItems().stream()
            .filter(
                item ->
                    item.getRoles().contains(rangerHelper.generateGravitinoRoleName(filesetRole)))
            .filter(
                item ->
                    item.getAccesses().stream()
                        .anyMatch(
                            access ->
                                access
                                    .getType()
                                    .equals(RangerPrivileges.RangerHdfsPrivilege.READ.getName())))
            .count());
    Assertions.assertEquals(
        0,
        policies.get(1).getPolicyItems().stream()
            .filter(
                item ->
                    item.getRoles().contains(rangerHelper.generateGravitinoRoleName(filesetRole)))
            .filter(
                item ->
                    item.getAccesses().stream()
                        .anyMatch(
                            access ->
                                access
                                    .getType()
                                    .equals(RangerPrivileges.RangerHdfsPrivilege.WRITE.getName())))
            .count());
    Assertions.assertEquals(
        1,
        policies.get(1).getPolicyItems().stream()
            .filter(
                item ->
                    item.getRoles().contains(rangerHelper.generateGravitinoRoleName(filesetRole)))
            .filter(
                item ->
                    item.getAccesses().stream()
                        .anyMatch(
                            access ->
                                access
                                    .getType()
                                    .equals(
                                        RangerPrivileges.RangerHdfsPrivilege.EXECUTE.getName())))
            .count());

    metalake.grantPrivilegesToRole(
        filesetRole,
        MetadataObjects.of(
            String.format("%s.%s", catalogName, schemaName),
            fileset.name(),
            MetadataObject.Type.FILESET),
        Sets.newHashSet(Privileges.WriteFileset.allow()));

    policies = rangerClient.getPoliciesInService(RangerITEnv.RANGER_HDFS_REPO_NAME);
    Assertions.assertEquals(2, policies.size());
    Assertions.assertEquals(3, policies.get(1).getPolicyItems().size());
    Assertions.assertEquals(
        1,
        policies.get(1).getPolicyItems().stream()
            .filter(
                item ->
                    item.getRoles().contains(rangerHelper.generateGravitinoRoleName(filesetRole)))
            .filter(
                item ->
                    item.getAccesses().stream()
                        .anyMatch(
                            access ->
                                access
                                    .getType()
                                    .equals(RangerPrivileges.RangerHdfsPrivilege.READ.getName())))
            .count());
    Assertions.assertEquals(
        1,
        policies.get(1).getPolicyItems().stream()
            .filter(
                item ->
                    item.getRoles().contains(rangerHelper.generateGravitinoRoleName(filesetRole)))
            .filter(
                item ->
                    item.getAccesses().stream()
                        .anyMatch(
                            access ->
                                access
                                    .getType()
                                    .equals(RangerPrivileges.RangerHdfsPrivilege.WRITE.getName())))
            .count());
    Assertions.assertEquals(
        1,
        policies.get(1).getPolicyItems().stream()
            .filter(
                item ->
                    item.getRoles().contains(rangerHelper.generateGravitinoRoleName(filesetRole)))
            .filter(
                item ->
                    item.getAccesses().stream()
                        .anyMatch(
                            access ->
                                access
                                    .getType()
                                    .equals(
                                        RangerPrivileges.RangerHdfsPrivilege.EXECUTE.getName())))
            .count());

    metalake.revokePrivilegesFromRole(
        filesetRole,
        MetadataObjects.of(
            String.format("%s.%s", catalogName, schemaName),
            fileset.name(),
            MetadataObject.Type.FILESET),
        Sets.newHashSet(Privileges.ReadFileset.allow(), Privileges.WriteFileset.allow()));
    policies = rangerClient.getPoliciesInService(RangerITEnv.RANGER_HDFS_REPO_NAME);
    Assertions.assertEquals(2, policies.size());
    Assertions.assertEquals(3, policies.get(0).getPolicyItems().size());
    Assertions.assertEquals(
        0,
        policies.get(1).getPolicyItems().stream()
            .filter(
                item ->
                    item.getRoles().contains(rangerHelper.generateGravitinoRoleName(filesetRole)))
            .filter(
                item ->
                    item.getAccesses().stream()
                        .anyMatch(
                            access ->
                                access
                                    .getType()
                                    .equals(RangerPrivileges.RangerHdfsPrivilege.READ.getName())))
            .count());
    Assertions.assertEquals(
        0,
        policies.get(1).getPolicyItems().stream()
            .filter(
                item ->
                    item.getRoles().contains(rangerHelper.generateGravitinoRoleName(filesetRole)))
            .filter(
                item ->
                    item.getAccesses().stream()
                        .anyMatch(
                            access ->
                                access
                                    .getType()
                                    .equals(RangerPrivileges.RangerHdfsPrivilege.WRITE.getName())))
            .count());
    Assertions.assertEquals(
        0,
        policies.get(1).getPolicyItems().stream()
            .filter(
                item ->
                    item.getRoles().contains(rangerHelper.generateGravitinoRoleName(filesetRole)))
            .filter(
                item ->
                    item.getAccesses().stream()
                        .anyMatch(
                            access ->
                                access
                                    .getType()
                                    .equals(
                                        RangerPrivileges.RangerHdfsPrivilege.EXECUTE.getName())))
            .count());

    catalog.asFilesetCatalog().dropFileset(NameIdentifier.of(schemaName, fileset.name()));
    policies = rangerClient.getPoliciesInService(RangerITEnv.RANGER_HDFS_REPO_NAME);
    Assertions.assertEquals(1, policies.size());
  }

  @Test
  @Order(1)
  void testReadWritePathE2E() throws IOException, RangerServiceException, InterruptedException {
    String filenameRole = GravitinoITUtils.genRandomName("RangerFilesetE2EIT_fileset");
    Fileset fileset =
        catalog
            .asFilesetCatalog()
            .createFileset(
                NameIdentifier.of(schemaName, filenameRole),
                "comment",
                Fileset.Type.MANAGED,
                storageLocation(filenameRole),
                null);
    Assertions.assertTrue(
        catalog.asFilesetCatalog().filesetExists(NameIdentifier.of(schemaName, fileset.name())));
    Assertions.assertTrue(fileSystem.exists(new Path(storageLocation(filenameRole))));
    FsPermission permission = new FsPermission("700");
    fileSystem.setPermission(new Path(storageLocation(filenameRole)), permission);

    String userName = "userTestReadWritePathE2E";
    metalake.addUser(userName);

    UserGroupInformation.createProxyUser(userName, UserGroupInformation.getCurrentUser())
        .doAs(
            (PrivilegedExceptionAction<Void>)
                () -> {
                  Configuration conf = new Configuration();
                  conf.set("fs.defaultFS", defaultBaseLocation());
                  FileSystem userFileSystem = FileSystem.get(conf);
                  Assertions.assertThrows(
                      Exception.class,
                      () ->
                          userFileSystem.listFiles(new Path(storageLocation(filenameRole)), false));
                  Assertions.assertThrows(
                      Exception.class,
                      () ->
                          userFileSystem.mkdirs(
                              new Path(
                                  String.format("%s/%s", storageLocation(filenameRole), "test1"))));
                  userFileSystem.close();
                  return null;
                });

    String filesetRole = currentFunName() + "_testReadWritePathE2E";
    SecurableObject securableObject =
        SecurableObjects.parse(
            String.format("%s.%s.%s", catalogName, schemaName, fileset.name()),
            MetadataObject.Type.FILESET,
            Lists.newArrayList(Privileges.ReadFileset.allow()));
    metalake.createRole(filesetRole, Collections.emptyMap(), Lists.newArrayList(securableObject));
    metalake.grantRolesToUser(Lists.newArrayList(filesetRole), userName);
    RangerBaseE2EIT.waitForUpdatingPolicies();

    UserGroupInformation.createProxyUser(userName, UserGroupInformation.getCurrentUser())
        .doAs(
            (PrivilegedExceptionAction<Void>)
                () -> {
                  FileSystem userFileSystem =
                      FileSystem.get(
                          new Configuration() {
                            {
                              set("fs.defaultFS", defaultBaseLocation());
                            }
                          });
                  Assertions.assertDoesNotThrow(
                      () ->
                          userFileSystem.listFiles(new Path(storageLocation(filenameRole)), false));
                  Assertions.assertThrows(
                      Exception.class,
                      () ->
                          userFileSystem.mkdirs(
                              new Path(
                                  String.format("%s/%s", storageLocation(filenameRole), "test2"))));
                  userFileSystem.close();
                  return null;
                });

    MetadataObject filesetObject =
        MetadataObjects.of(
            String.format("%s.%s", catalogName, schemaName),
            fileset.name(),
            MetadataObject.Type.FILESET);
    metalake.grantPrivilegesToRole(
        filesetRole, filesetObject, Sets.newHashSet(Privileges.WriteFileset.allow()));
    RangerBaseE2EIT.waitForUpdatingPolicies();
    UserGroupInformation.createProxyUser(userName, UserGroupInformation.getCurrentUser())
        .doAs(
            (PrivilegedExceptionAction<Void>)
                () -> {
                  FileSystem userFileSystem =
                      FileSystem.get(
                          new Configuration() {
                            {
                              set("fs.defaultFS", defaultBaseLocation());
                            }
                          });
                  Assertions.assertDoesNotThrow(
                      () ->
                          userFileSystem.listFiles(new Path(storageLocation(filenameRole)), false));
                  Assertions.assertDoesNotThrow(
                      () ->
                          userFileSystem.mkdirs(
                              new Path(
                                  String.format("%s/%s", storageLocation(filenameRole), "test3"))));
                  userFileSystem.close();
                  return null;
                });

    catalog
        .asFilesetCatalog()
        .alterFileset(
            NameIdentifier.of(schemaName, filenameRole), FilesetChange.rename("new_name"));
    UserGroupInformation.createProxyUser(userName, UserGroupInformation.getCurrentUser())
        .doAs(
            (PrivilegedExceptionAction<Void>)
                () -> {
                  FileSystem userFileSystem =
                      FileSystem.get(
                          new Configuration() {
                            {
                              set("fs.defaultFS", defaultBaseLocation());
                            }
                          });
                  Assertions.assertDoesNotThrow(
                      () ->
                          userFileSystem.listFiles(new Path(storageLocation(filenameRole)), false));
                  Assertions.assertDoesNotThrow(
                      () ->
                          userFileSystem.mkdirs(
                              new Path(
                                  String.format("%s/%s", storageLocation(filenameRole), "test3"))));
                  userFileSystem.close();
                  return null;
                });
    MetadataObject renamedFilesetObject =
        MetadataObjects.of(
            String.format("%s.%s", catalogName, schemaName),
            "new_name",
            MetadataObject.Type.FILESET);

    metalake.revokePrivilegesFromRole(
        filesetRole,
        renamedFilesetObject,
        Sets.newHashSet(Privileges.ReadFileset.allow(), Privileges.WriteFileset.allow()));
    RangerBaseE2EIT.waitForUpdatingPolicies();
    UserGroupInformation.createProxyUser(userName, UserGroupInformation.getCurrentUser())
        .doAs(
            (PrivilegedExceptionAction<Void>)
                () -> {
                  FileSystem userFileSystem =
                      FileSystem.get(
                          new Configuration() {
                            {
                              set("fs.defaultFS", defaultBaseLocation());
                            }
                          });
                  Assertions.assertThrows(
                      Exception.class,
                      () ->
                          userFileSystem.listFiles(new Path(storageLocation(filenameRole)), false));
                  Assertions.assertThrows(
                      Exception.class,
                      () ->
                          userFileSystem.mkdirs(
                              new Path(
                                  String.format("%s/%s", storageLocation(filenameRole), "test4"))));
                  userFileSystem.close();
                  return null;
                });

    catalog.asFilesetCatalog().dropFileset(NameIdentifier.of(schemaName, "new_name"));
  }

  private void createCatalogAndSchema() {
    GravitinoMetalake[] gravitinoMetalakes = client.listMetalakes();
    Assertions.assertEquals(0, gravitinoMetalakes.length);

    client.createMetalake(metalakeName, "comment", Collections.emptyMap());
    metalake = client.loadMetalake(metalakeName);
    Assertions.assertEquals(metalakeName, metalake.name());

    metalake.createCatalog(
        catalogName,
        Catalog.Type.FILESET,
        provider,
        "comment",
        ImmutableMap.of(
            IMPERSONATION_ENABLE,
            "true",
            AUTHORIZATION_PROVIDER,
            "ranger",
            RangerAuthorizationProperties.RANGER_SERVICE_TYPE,
            "HDFS",
            RangerAuthorizationProperties.RANGER_SERVICE_NAME,
            RangerITEnv.RANGER_HDFS_REPO_NAME,
            RangerAuthorizationProperties.RANGER_ADMIN_URL,
            RANGER_ADMIN_URL,
            RangerAuthorizationProperties.RANGER_AUTH_TYPE,
            RangerContainer.authType,
            RangerAuthorizationProperties.RANGER_USERNAME,
            RangerContainer.rangerUserName,
            RangerAuthorizationProperties.RANGER_PASSWORD,
            RangerContainer.rangerPassword));

    catalog = metalake.loadCatalog(catalogName);
    catalog
        .asSchemas()
        .createSchema(schemaName, "comment", ImmutableMap.of("location", defaultBaseLocation()));
    Schema loadSchema = catalog.asSchemas().loadSchema(schemaName);
    Assertions.assertEquals(schemaName, loadSchema.name());
    Assertions.assertNotNull(loadSchema.properties().get("location"));
  }

  private String defaultBaseLocation() {
    if (defaultBaseLocation == null) {
      defaultBaseLocation =
          String.format(
              "hdfs://%s:%d/user/hadoop/%s",
              containerSuite.getHiveRangerContainer().getContainerIpAddress(),
              HiveContainer.HDFS_DEFAULTFS_PORT,
              schemaName.toLowerCase());
    }
    return defaultBaseLocation;
  }

  private String storageLocation(String filesetName) {
    return defaultBaseLocation() + "/" + filesetName;
  }
}
