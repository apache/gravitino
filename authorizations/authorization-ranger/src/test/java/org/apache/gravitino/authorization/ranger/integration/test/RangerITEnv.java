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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.gravitino.authorization.Role;
import org.apache.gravitino.authorization.ranger.RangerAuthorizationHivePlugin;
import org.apache.gravitino.authorization.ranger.RangerAuthorizationPlugin;
import org.apache.gravitino.authorization.ranger.RangerHelper;
import org.apache.gravitino.authorization.ranger.RangerPrivileges;
import org.apache.gravitino.authorization.ranger.RangerSecurableObject;
import org.apache.gravitino.authorization.ranger.reference.RangerDefines;
import org.apache.gravitino.connector.AuthorizationPropertiesMeta;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.container.HiveContainer;
import org.apache.gravitino.integration.test.container.RangerContainer;
import org.apache.gravitino.integration.test.container.TrinoContainer;
import org.apache.ranger.RangerClient;
import org.apache.ranger.RangerServiceException;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerRole;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.util.SearchFilter;
import org.junit.jupiter.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Ranger IT environment
public class RangerITEnv {
  private static final Logger LOG = LoggerFactory.getLogger(RangerITEnv.class);
  protected static final String RANGER_TRINO_REPO_NAME = "trinoDev";
  private static final String RANGER_TRINO_TYPE = "trino";
  protected static final String RANGER_HIVE_REPO_NAME = "hiveDev";
  private static final String RANGER_HIVE_TYPE = "hive";
  protected static final String RANGER_HDFS_REPO_NAME = "hdfsDev";
  private static final String RANGER_HDFS_TYPE = "hdfs";
  protected static RangerClient rangerClient;
  protected static final String HADOOP_USER_NAME = "gravitino";
  private static volatile boolean initRangerService = false;
  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();

  // Hive resource database name
  public static final String RESOURCE_DATABASE = "database";
  // Hive resource table name
  public static final String RESOURCE_TABLE = "table";
  // Hive resource column name
  public static final String RESOURCE_COLUMN = "column";
  // HDFS resource path name
  public static final String RESOURCE_PATH = "path";
  public static final String SEARCH_FILTER_DATABASE =
      SearchFilter.RESOURCE_PREFIX + RESOURCE_DATABASE;
  // Search filter prefix table constants
  public static final String SEARCH_FILTER_TABLE = SearchFilter.RESOURCE_PREFIX + RESOURCE_TABLE;
  // Search filter prefix column constants
  public static final String SEARCH_FILTER_COLUMN = SearchFilter.RESOURCE_PREFIX + RESOURCE_COLUMN;
  // Search filter prefix file path constants
  public static final String SEARCH_FILTER_PATH = SearchFilter.RESOURCE_PREFIX + RESOURCE_PATH;
  public static RangerAuthorizationPlugin rangerAuthHivePlugin;
  protected static RangerHelper rangerHelper;

  public static void init() {
    containerSuite.startRangerContainer();
    rangerClient = containerSuite.getRangerContainer().rangerClient;

    rangerAuthHivePlugin =
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
    rangerHelper =
        new RangerHelper(
            rangerClient,
            RangerContainer.rangerUserName,
            RangerITEnv.RANGER_HIVE_REPO_NAME,
            rangerAuthHivePlugin.ownerMappingRule(),
            rangerAuthHivePlugin.policyResourceDefinesRule());

    if (!initRangerService) {
      synchronized (RangerITEnv.class) {
        // No IP address set, no impact on testing
        createRangerHdfsRepository("", true);
        createRangerHiveRepository("", true);
        allowAnyoneAccessHDFS();
        allowAnyoneAccessInformationSchema();
        initRangerService = true;
      }
    }
  }

  public static void cleanup() {
    try {
      if (rangerClient != null) {
        if (rangerClient.getService(RANGER_TRINO_REPO_NAME) != null) {
          rangerClient.deleteService(RANGER_TRINO_REPO_NAME);
        }
        if (rangerClient.getService(RANGER_HIVE_REPO_NAME) != null) {
          rangerClient.deleteService(RANGER_HIVE_REPO_NAME);
        }
      }
    } catch (RangerServiceException e) {
      // ignore
    }
  }

  static void startHiveRangerContainer() {
    containerSuite.startHiveRangerContainer(
        new HashMap<>(
            ImmutableMap.of(
                HiveContainer.HIVE_RUNTIME_VERSION,
                HiveContainer.HIVE3,
                RangerContainer.DOCKER_ENV_RANGER_SERVER_URL,
                String.format(
                    "http://%s:%d",
                    containerSuite.getRangerContainer().getContainerIpAddress(),
                    RangerContainer.RANGER_SERVER_PORT),
                RangerContainer.DOCKER_ENV_RANGER_HIVE_REPOSITORY_NAME,
                RangerITEnv.RANGER_HIVE_REPO_NAME,
                RangerContainer.DOCKER_ENV_RANGER_HDFS_REPOSITORY_NAME,
                RangerITEnv.RANGER_HDFS_REPO_NAME,
                HiveContainer.HADOOP_USER_NAME,
                HADOOP_USER_NAME)));
  }

  /** Currently we only test Ranger Hive, So wo Allow anyone to visit HDFS */
  static void allowAnyoneAccessHDFS() {
    String policyName = currentFunName();
    try {
      if (null != rangerClient.getPolicy(RANGER_HDFS_REPO_NAME, policyName)) {
        return;
      }
    } catch (RangerServiceException e) {
      // If the policy doesn't exist, we will create it
      LOG.warn("Error while fetching policy: {}", e.getMessage());
    }

    Map<String, RangerPolicy.RangerPolicyResource> policyResourceMap =
        ImmutableMap.of("path", new RangerPolicy.RangerPolicyResource("/*"));
    RangerPolicy.RangerPolicyItem policyItem = new RangerPolicy.RangerPolicyItem();
    policyItem.setUsers(Arrays.asList(RangerDefines.CURRENT_USER));
    policyItem.setAccesses(
        Arrays.asList(
            new RangerPolicy.RangerPolicyItemAccess(
                RangerPrivileges.RangerHdfsPrivilege.READ.toString()),
            new RangerPolicy.RangerPolicyItemAccess(
                RangerPrivileges.RangerHdfsPrivilege.WRITE.toString()),
            new RangerPolicy.RangerPolicyItemAccess(
                RangerPrivileges.RangerHdfsPrivilege.EXECUTE.toString())));
    updateOrCreateRangerPolicy(
        RANGER_HDFS_TYPE,
        RANGER_HDFS_REPO_NAME,
        policyName,
        policyResourceMap,
        Collections.singletonList(policyItem));
  }

  /**
   * Hive must have this policy Allow anyone can access information schema to show `database`,
   * `tables` and `columns`
   */
  static void allowAnyoneAccessInformationSchema() {
    String policyName = currentFunName();
    try {
      if (null != rangerClient.getPolicy(RANGER_HIVE_REPO_NAME, policyName)) {
        return;
      }
    } catch (RangerServiceException e) {
      // If the policy doesn't exist, we will create it
      LOG.warn("Error while fetching policy: {}", e.getMessage());
    }

    Map<String, RangerPolicy.RangerPolicyResource> policyResourceMap =
        ImmutableMap.of(
            "database",
            new RangerPolicy.RangerPolicyResource("information_schema"),
            "table",
            new RangerPolicy.RangerPolicyResource("*"),
            "column",
            new RangerPolicy.RangerPolicyResource("*"));
    RangerPolicy.RangerPolicyItem policyItem = new RangerPolicy.RangerPolicyItem();
    policyItem.setGroups(Arrays.asList(RangerDefines.PUBLIC_GROUP));
    policyItem.setAccesses(
        Arrays.asList(
            new RangerPolicy.RangerPolicyItemAccess(
                RangerPrivileges.RangerHivePrivilege.SELECT.toString())));
    updateOrCreateRangerPolicy(
        RANGER_HIVE_TYPE,
        RANGER_HIVE_REPO_NAME,
        policyName,
        policyResourceMap,
        Collections.singletonList(policyItem));
  }

  public void createRangerTrinoRepository(String trinoIp) {
    String usernameKey = "username";
    String usernameVal = "admin";
    String jdbcKey = "jdbc.driverClassName";
    String jdbcVal = "io.trino.jdbc.TrinoDriver";
    String jdbcUrlKey = "jdbc.url";
    String jdbcUrlVal = String.format("http:hive2://%s:%d", trinoIp, TrinoContainer.TRINO_PORT);

    RangerService service = new RangerService();
    service.setType(RANGER_TRINO_TYPE);
    service.setName(RANGER_TRINO_REPO_NAME);
    service.setConfigs(
        ImmutableMap.<String, String>builder()
            .put(usernameKey, usernameVal)
            .put(jdbcKey, jdbcVal)
            .put(jdbcUrlKey, jdbcUrlVal)
            .build());

    try {
      RangerService createdService = rangerClient.createService(service);
      Assertions.assertNotNull(createdService);

      Map<String, String> filter =
          ImmutableMap.of(SearchFilter.SERVICE_NAME, RANGER_TRINO_REPO_NAME);
      List<RangerService> services = rangerClient.findServices(filter);
      Assertions.assertEquals(RANGER_TRINO_TYPE, services.get(0).getType());
      Assertions.assertEquals(RANGER_TRINO_REPO_NAME, services.get(0).getName());
      Assertions.assertEquals(usernameVal, services.get(0).getConfigs().get(usernameKey));
      Assertions.assertEquals(jdbcVal, services.get(0).getConfigs().get(jdbcKey));
      Assertions.assertEquals(jdbcUrlVal, services.get(0).getConfigs().get(jdbcUrlKey));
    } catch (RangerServiceException e) {
      throw new RuntimeException(e);
    }
  }

  public static void createRangerHiveRepository(String hiveIp, boolean cleanAllPolicy) {
    try {
      if (null != rangerClient.getService(RANGER_HIVE_REPO_NAME)) {
        return;
      }
    } catch (RangerServiceException e) {
      LOG.warn("Error while fetching service: {}", e.getMessage());
    }

    String usernameKey = "username";
    String usernameVal = "admin";
    String passwordKey = "password";
    String passwordVal = "admin";
    String jdbcKey = "jdbc.driverClassName";
    String jdbcVal = "org.apache.hive.jdbc.HiveDriver";
    String jdbcUrlKey = "jdbc.url";
    String jdbcUrlVal =
        String.format("jdbc:hive2://%s:%d", hiveIp, HiveContainer.HIVE_SERVICE_PORT);

    RangerService service = new RangerService();
    service.setType(RANGER_HIVE_TYPE);
    service.setName(RANGER_HIVE_REPO_NAME);
    service.setConfigs(
        ImmutableMap.<String, String>builder()
            .put(usernameKey, usernameVal)
            .put(passwordKey, passwordVal)
            .put(jdbcKey, jdbcVal)
            .put(jdbcUrlKey, jdbcUrlVal)
            .build());

    try {
      RangerService createdService = rangerClient.createService(service);
      Assertions.assertNotNull(createdService);

      Map<String, String> filter =
          ImmutableMap.of(SearchFilter.SERVICE_NAME, RANGER_HIVE_REPO_NAME);
      List<RangerService> services = rangerClient.findServices(filter);
      Assertions.assertEquals(RANGER_HIVE_TYPE, services.get(0).getType());
      Assertions.assertEquals(RANGER_HIVE_REPO_NAME, services.get(0).getName());
      Assertions.assertEquals(usernameVal, services.get(0).getConfigs().get(usernameKey));
      Assertions.assertEquals(jdbcVal, services.get(0).getConfigs().get(jdbcKey));
      Assertions.assertEquals(jdbcUrlVal, services.get(0).getConfigs().get(jdbcUrlKey));

      if (cleanAllPolicy) {
        cleanAllPolicy(RANGER_HIVE_REPO_NAME);
      }
    } catch (RangerServiceException e) {
      throw new RuntimeException(e);
    }
  }

  public static void createRangerHdfsRepository(String hdfsIp, boolean cleanAllPolicy) {
    try {
      if (null != rangerClient.getService(RANGER_HDFS_REPO_NAME)) {
        return;
      }
    } catch (RangerServiceException e) {
      LOG.warn("Error while fetching service: {}", e.getMessage());
    }

    String usernameKey = "username";
    String usernameVal = "admin";
    String passwordKey = "password";
    String passwordVal = "admin";
    String authenticationKey = "hadoop.security.authentication";
    String authenticationVal = "simple";
    String protectionKey = "hadoop.rpc.protection";
    String protectionVal = "authentication";
    String authorizationKey = "hadoop.security.authorization";
    String authorizationVal = "false";
    String fsDefaultNameKey = "fs.default.name";
    String fsDefaultNameVal =
        String.format("hdfs://%s:%d", hdfsIp, HiveContainer.HDFS_DEFAULTFS_PORT);

    RangerService service = new RangerService();
    service.setType(RANGER_HDFS_TYPE);
    service.setName(RANGER_HDFS_REPO_NAME);
    service.setConfigs(
        ImmutableMap.<String, String>builder()
            .put(usernameKey, usernameVal)
            .put(passwordKey, passwordVal)
            .put(authenticationKey, authenticationVal)
            .put(protectionKey, protectionVal)
            .put(authorizationKey, authorizationVal)
            .put(fsDefaultNameKey, fsDefaultNameVal)
            .build());

    try {
      RangerService createdService = rangerClient.createService(service);
      Assertions.assertNotNull(createdService);

      Map<String, String> filter =
          ImmutableMap.of(SearchFilter.SERVICE_NAME, RANGER_HDFS_REPO_NAME);
      List<RangerService> services = rangerClient.findServices(filter);
      Assertions.assertEquals(RANGER_HDFS_TYPE, services.get(0).getType());
      Assertions.assertEquals(RANGER_HDFS_REPO_NAME, services.get(0).getName());
      Assertions.assertEquals(usernameVal, services.get(0).getConfigs().get(usernameKey));
      Assertions.assertEquals(
          authenticationVal, services.get(0).getConfigs().get(authenticationKey));
      Assertions.assertEquals(protectionVal, services.get(0).getConfigs().get(protectionKey));
      Assertions.assertEquals(authorizationVal, services.get(0).getConfigs().get(authorizationKey));
      Assertions.assertEquals(fsDefaultNameVal, services.get(0).getConfigs().get(fsDefaultNameKey));

      if (cleanAllPolicy) {
        cleanAllPolicy(RANGER_HDFS_REPO_NAME);
      }
    } catch (RangerServiceException e) {
      throw new RuntimeException(e);
    }
  }

  protected static void verifyRoleInRanger(
      RangerAuthorizationPlugin rangerAuthPlugin,
      Role role,
      List<String> includeUsers,
      List<String> excludeUsers,
      List<String> includeGroups,
      List<String> excludeGroups) {
    // Verify role in RangerRole
    RangerRole rangerRole = null;
    try {
      rangerRole =
          RangerITEnv.rangerClient.getRole(
              role.name(), rangerAuthPlugin.rangerAdminName, RangerITEnv.RANGER_HIVE_REPO_NAME);
      LOG.info("rangerRole: " + rangerRole.toString());
    } catch (RangerServiceException e) {
      throw new RuntimeException(e);
    }
    rangerRole
        .getUsers()
        .forEach(
            user -> {
              if (includeUsers != null && !includeUsers.isEmpty()) {
                Assertions.assertTrue(
                    includeUsers.contains(user.getName()),
                    "includeUsersInRole: " + includeUsers + ", user: " + user.getName());
              }
              if (excludeUsers != null && !excludeUsers.isEmpty()) {
                Assertions.assertFalse(
                    excludeUsers.contains(user.getName()),
                    "excludeUsersInRole: " + excludeUsers.toString() + ", user: " + user.getName());
              }
            });
    rangerRole
        .getGroups()
        .forEach(
            group -> {
              if (includeGroups != null && !includeGroups.isEmpty()) {
                Assertions.assertTrue(
                    includeGroups.contains(group.getName()),
                    "includeGroupsInRole: "
                        + includeGroups.toString()
                        + ", group: "
                        + group.getName());
              }
              if (excludeGroups != null && !excludeGroups.isEmpty()) {
                Assertions.assertFalse(
                    excludeGroups.contains(group.getName()),
                    "excludeGroupsInRole: "
                        + excludeGroups.toString()
                        + ", group: "
                        + group.getName());
              }
            });

    // Verify role in RangerPolicy
    role.securableObjects()
        .forEach(
            securableObject -> {
              List<RangerSecurableObject> rangerSecurableObjects =
                  rangerAuthPlugin.translatePrivilege(securableObject);

              rangerSecurableObjects.forEach(
                  rangerSecurableObject -> {
                    RangerPolicy policy;
                    try {
                      policy =
                          RangerITEnv.rangerClient.getPolicy(
                              RangerITEnv.RANGER_HIVE_REPO_NAME, rangerSecurableObject.fullName());
                      LOG.info("policy: " + policy.toString());
                    } catch (RangerServiceException e) {
                      LOG.error("Failed to get policy: " + securableObject.fullName());
                      throw new RuntimeException(e);
                    }
                    boolean match =
                        policy.getPolicyItems().stream()
                            .filter(
                                policyItem -> {
                                  // Filter Ranger policy item by Gravitino privilege
                                  return policyItem.getAccesses().stream()
                                      .anyMatch(
                                          access -> {
                                            return rangerSecurableObject
                                                .privileges()
                                                .contains(
                                                    RangerPrivileges.valueOf(access.getType()));
                                          });
                                })
                            .allMatch(
                                policyItem -> {
                                  // Verify role name in Ranger policy item
                                  return policyItem.getRoles().contains(role.name());
                                });
                    Assertions.assertTrue(match);
                  });
            });
  }

  protected static void verifyRoleInRanger(RangerAuthorizationPlugin rangerAuthPlugin, Role role) {
    RangerITEnv.verifyRoleInRanger(rangerAuthPlugin, role, null, null, null, null);
  }

  protected static void verifyRoleInRanger(
      RangerAuthorizationPlugin rangerAuthPlugin,
      Role role,
      List<String> includeRolesInPolicyItem) {
    RangerITEnv.verifyRoleInRanger(
        rangerAuthPlugin, role, includeRolesInPolicyItem, null, null, null);
  }

  protected static void verifyRoleInRanger(
      RangerAuthorizationPlugin rangerAuthPlugin,
      Role role,
      List<String> includeRolesInPolicyItem,
      List<String> excludeRolesInPolicyItem) {
    RangerITEnv.verifyRoleInRanger(
        rangerAuthPlugin, role, includeRolesInPolicyItem, excludeRolesInPolicyItem, null, null);
  }

  protected static void verifyRoleInRanger(
      RangerAuthorizationPlugin rangerAuthPlugin,
      Role role,
      List<String> includeUsers,
      List<String> excludeUsers,
      List<String> includeGroups) {
    RangerITEnv.verifyRoleInRanger(
        rangerAuthPlugin, role, includeUsers, excludeUsers, includeGroups, null);
  }

  protected static void updateOrCreateRangerPolicy(
      String type,
      String serviceName,
      String policyName,
      Map<String, RangerPolicy.RangerPolicyResource> policyResourceMap,
      List<RangerPolicy.RangerPolicyItem> policyItems) {

    Map<String, String> resourceFilter = new HashMap<>(); // use to match the precise policy
    Map<String, String> policyFilter = new HashMap<>();
    policyFilter.put(SearchFilter.SERVICE_NAME, serviceName);
    policyFilter.put(SearchFilter.POLICY_LABELS_PARTIAL, RangerHelper.MANAGED_BY_GRAVITINO);
    final int[] index = {0};
    policyResourceMap.forEach(
        (k, v) -> {
          if (type.equals(RANGER_HIVE_TYPE)) {
            if (index[0] == 0) {
              policyFilter.put(SEARCH_FILTER_DATABASE, v.getValues().get(0));
              resourceFilter.put(RESOURCE_DATABASE, v.getValues().get(0));
            } else if (index[0] == 1) {
              policyFilter.put(SEARCH_FILTER_TABLE, v.getValues().get(0));
              resourceFilter.put(RESOURCE_TABLE, v.getValues().get(0));
            } else if (index[0] == 2) {
              policyFilter.put(SEARCH_FILTER_COLUMN, v.getValues().get(0));
              resourceFilter.put(RESOURCE_TABLE, v.getValues().get(0));
            }
            index[0]++;
          } else if (type.equals(RANGER_HDFS_TYPE)) {
            policyFilter.put(SEARCH_FILTER_PATH, v.getValues().get(0));
            resourceFilter.put(RESOURCE_PATH, v.getValues().get(0));
          }
        });
    try {
      List<RangerPolicy> policies = rangerClient.findPolicies(policyFilter);
      if (!policies.isEmpty()) {
        // Because Ranger doesn't support the precise search, Ranger will return the policy meets
        // the wildcard(*,?) conditions, If you use `db.table` condition to search policy, the
        // Ranger will match `db1.table1`, `db1.table2`, `db*.table*`, So we need to manually
        // precisely filter this research results.
        policies =
            policies.stream()
                .filter(
                    policy ->
                        policy.getResources().entrySet().stream()
                            .allMatch(
                                entry ->
                                    resourceFilter.containsKey(entry.getKey())
                                        && entry.getValue().getValues().size() == 1
                                        && entry
                                            .getValue()
                                            .getValues()
                                            .contains(resourceFilter.get(entry.getKey()))))
                .collect(Collectors.toList());
      }

      Assertions.assertTrue(policies.size() <= 1);
      if (!policies.isEmpty()) {
        RangerPolicy policy = policies.get(0);
        policy.getPolicyItems().addAll(policyItems);
        rangerClient.updatePolicy(policy.getId(), policy);
      } else {
        RangerPolicy policy = new RangerPolicy();
        policy.setServiceType(type);
        policy.setService(serviceName);
        policy.setName(policyName);
        policy.setPolicyLabels(Lists.newArrayList(RangerHelper.MANAGED_BY_GRAVITINO));
        policy.setResources(policyResourceMap);
        policy.setPolicyItems(policyItems);
        rangerClient.createPolicy(policy);
      }
    } catch (RangerServiceException e) {
      throw new RuntimeException(e);
    }

    try {
      Thread.sleep(
          1000); // Sleep for a while to wait for the Hive/HDFS Ranger plugin to be updated policy.
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /** Clean all policy in the Ranger */
  protected static void cleanAllPolicy(String serviceName) {
    try {
      List<RangerPolicy> policies =
          rangerClient.findPolicies(ImmutableMap.of(SearchFilter.SERVICE_NAME, serviceName));
      for (RangerPolicy policy : policies) {
        rangerClient.deletePolicy(policy.getId());
      }
    } catch (RangerServiceException e) {
      throw new RuntimeException(e);
    }
  }

  /** Don't call this function in the Lambda function body, It will return a random function name */
  public static String currentFunName() {
    return Thread.currentThread().getStackTrace()[2].getMethodName();
  }
}
