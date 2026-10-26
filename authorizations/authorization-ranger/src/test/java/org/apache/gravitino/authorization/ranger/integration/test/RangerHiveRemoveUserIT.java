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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Configs;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.auth.AuthenticatorType;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.authorization.common.RangerAuthorizationProperties;
import org.apache.gravitino.authorization.ranger.RangerHelper;
import org.apache.gravitino.catalog.hive.HiveConstants;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.integration.test.container.HiveContainer;
import org.apache.gravitino.integration.test.container.RangerContainer;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.ranger.RangerServiceException;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerRole;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Checks against a real Ranger and Hive that removing a user or group from Gravitino also removes
 * it from the Ranger roles it was granted through Gravitino.
 *
 * <p>Granting a role adds the principal to the Ranger role {@code GRAVITINO_<role>}, whose policy
 * items carry the role's privileges. If the removal does not revoke that membership, the removed
 * principal keeps the role's access in Ranger.
 */
@Tag("gravitino-docker-test")
public class RangerHiveRemoveUserIT extends BaseIT {
  private static final Logger LOG = LoggerFactory.getLogger(RangerHiveRemoveUserIT.class);

  private static final String PROVIDER = "hive";
  private static final String SELECT_ACCESS = "select";

  private final String metalakeName =
      GravitinoITUtils.genRandomName("remove_user_metalake").toLowerCase();
  private final String catalogName =
      GravitinoITUtils.genRandomName("remove_user_catalog").toLowerCase();

  private GravitinoMetalake metalake;

  @BeforeAll
  @Override
  public void startIntegrationTest() throws Exception {
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

    RangerITEnv.init(metalakeName, true);
    RangerITEnv.startHiveRangerContainer();
    RangerITEnv.cleanup();

    String metastoreUris =
        String.format(
            "thrift://%s:%d",
            containerSuite.getHiveRangerContainer().getContainerIpAddress(),
            HiveContainer.HIVE_METASTORE_PORT);

    client.createMetalake(metalakeName, "comment", Collections.emptyMap());
    metalake = client.loadMetalake(metalakeName);

    Map<String, String> properties =
        ImmutableMap.<String, String>builder()
            .put(HiveConstants.METASTORE_URIS, metastoreUris)
            .put(AUTHORIZATION_PROVIDER, "ranger")
            .put(RangerAuthorizationProperties.RANGER_SERVICE_TYPE, "HadoopSQL")
            .put(
                RangerAuthorizationProperties.RANGER_SERVICE_NAME,
                RangerITEnv.RANGER_HIVE_REPO_NAME)
            .put(RangerAuthorizationProperties.RANGER_ADMIN_URL, RangerITEnv.RANGER_ADMIN_URL)
            .put(RangerAuthorizationProperties.RANGER_AUTH_TYPE, RangerContainer.authType)
            .put(RangerAuthorizationProperties.RANGER_USERNAME, RangerContainer.rangerUserName)
            .put(RangerAuthorizationProperties.RANGER_PASSWORD, RangerContainer.rangerPassword)
            .build();
    Catalog catalog =
        metalake.createCatalog(
            catalogName, Catalog.Type.RELATIONAL, PROVIDER, "comment", properties);
    LOG.info("Catalog created: {}", catalog);
  }

  @AfterAll
  public void stop() {
    if (client != null) {
      try {
        metalake.dropCatalog(catalogName, true);
        client.disableMetalake(metalakeName);
        client.dropMetalake(metalakeName);
      } catch (Exception e) {
        LOG.error("Failed to clean up the metalake {}", metalakeName, e);
      }
    }
    RangerITEnv.cleanup();
  }

  @Test
  public void testRemoveUserRevokesRangerRoleMembership() throws RangerServiceException {
    String userName = "removed_user";
    String roleName = "removed_user_role";
    createCatalogRole(roleName);
    metalake.addUser(userName);
    metalake.grantRolesToUser(Lists.newArrayList(roleName), userName);

    Assertions.assertTrue(
        rangerRoleUsers(roleName).contains(userName),
        "Granting the role must add the user to the Ranger role");
    Assertions.assertTrue(hasRolePolicyItem(roleName));

    Assertions.assertTrue(metalake.removeUser(userName));

    Assertions.assertFalse(
        rangerRoleUsers(roleName).contains(userName),
        "Removing the user must remove it from the Ranger role");
    // Only the membership goes away: the role and its privileges stay for its other holders.
    Assertions.assertTrue(hasRolePolicyItem(roleName));
  }

  @Test
  public void testRemoveGroupRevokesRangerRoleMembership() throws RangerServiceException {
    String groupName = "removed_group";
    String roleName = "removed_group_role";
    createCatalogRole(roleName);
    metalake.addGroup(groupName);
    metalake.grantRolesToGroup(Lists.newArrayList(roleName), groupName);

    Assertions.assertTrue(
        rangerRoleGroups(roleName).contains(groupName),
        "Granting the role must add the group to the Ranger role");
    Assertions.assertTrue(hasRolePolicyItem(roleName));

    Assertions.assertTrue(metalake.removeGroup(groupName));

    Assertions.assertFalse(
        rangerRoleGroups(roleName).contains(groupName),
        "Removing the group must remove it from the Ranger role");
    Assertions.assertTrue(hasRolePolicyItem(roleName));
  }

  private void createCatalogRole(String roleName) {
    SecurableObject catalogObject =
        SecurableObjects.ofCatalog(
            catalogName,
            Lists.newArrayList(
                Privileges.UseCatalog.allow(),
                Privileges.UseSchema.allow(),
                Privileges.SelectTable.allow()));
    metalake.createRole(roleName, Collections.emptyMap(), Lists.newArrayList(catalogObject));
  }

  private static RangerRole rangerRole(String roleName) throws RangerServiceException {
    RangerRole rangerRole =
        RangerITEnv.rangerClient.getRole(
            RangerHelper.GRAVITINO_ROLE_PREFIX + roleName,
            RangerContainer.rangerUserName,
            RangerITEnv.RANGER_HIVE_REPO_NAME);
    LOG.info("Ranger role: {}", rangerRole);
    return rangerRole;
  }

  private static List<String> rangerRoleUsers(String roleName) throws RangerServiceException {
    return rangerRole(roleName).getUsers().stream()
        .map(RangerRole.RoleMember::getName)
        .collect(Collectors.toList());
  }

  private static List<String> rangerRoleGroups(String roleName) throws RangerServiceException {
    return rangerRole(roleName).getGroups().stream()
        .map(RangerRole.RoleMember::getName)
        .collect(Collectors.toList());
  }

  /** Whether some Ranger policy of the service allows `select` to the Gravitino role. */
  private static boolean hasRolePolicyItem(String roleName) throws RangerServiceException {
    String rangerRoleName = RangerHelper.GRAVITINO_ROLE_PREFIX + roleName;
    List<RangerPolicy> policies =
        RangerITEnv.rangerClient.getPoliciesInService(RangerITEnv.RANGER_HIVE_REPO_NAME);
    return policies.stream()
        .flatMap(policy -> policy.getPolicyItems().stream())
        .anyMatch(
            item ->
                item.getRoles().contains(rangerRoleName)
                    && item.getAccesses().stream()
                        .anyMatch(access -> SELECT_ACCESS.equals(access.getType())));
  }
}
