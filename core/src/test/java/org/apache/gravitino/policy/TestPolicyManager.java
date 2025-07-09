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

package org.apache.gravitino.policy;

import static org.apache.gravitino.Configs.DEFAULT_ENTITY_RELATIONAL_STORE;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_MAX_CONNECTIONS;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_WAIT_MILLISECONDS;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_STORE;
import static org.apache.gravitino.Configs.ENTITY_STORE;
import static org.apache.gravitino.Configs.RELATIONAL_ENTITY_STORE;
import static org.apache.gravitino.Configs.STORE_DELETE_AFTER_TIME;
import static org.apache.gravitino.Configs.STORE_TRANSACTION_MAX_SKEW_TIME;
import static org.apache.gravitino.Configs.TREE_LOCK_CLEAN_INTERVAL;
import static org.apache.gravitino.Configs.TREE_LOCK_MAX_NODE_IN_MEMORY;
import static org.apache.gravitino.Configs.TREE_LOCK_MIN_NODE_IN_MEMORY;
import static org.apache.gravitino.Configs.VERSION_RETENTION_COUNT;
import static org.apache.gravitino.policy.Policy.BUILT_IN_TYPE_PREFIX;
import static org.apache.gravitino.policy.Policy.SUPPORTS_ALL_OBJECT_TYPES;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.EntityStoreFactory;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchPolicyException;
import org.apache.gravitino.exceptions.PolicyAlreadyExistsException;
import org.apache.gravitino.lock.LockManager;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.SchemaVersion;
import org.apache.gravitino.metalake.MetalakeDispatcher;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestPolicyManager {
  private static final String METALAKE = "metalake_for_policy_test";
  private static final MetalakeDispatcher metalakeDispatcher = mock(MetalakeDispatcher.class);
  private static final String JDBC_STORE_PATH =
      "/tmp/gravitino_jdbc_entityStore_" + UUID.randomUUID().toString().replace("-", "");
  private static final String DB_DIR = JDBC_STORE_PATH + "/testdb";

  private static PolicyManager policyManager;

  @BeforeAll
  public static void setUp() throws IllegalAccessException, IOException {
    IdGenerator idGenerator = new RandomIdGenerator();
    Config config = mockConfig();
    EntityStore entityStore = EntityStoreFactory.createEntityStore(config);
    entityStore.initialize(config);
    policyManager = new PolicyManager(idGenerator, entityStore);

    FieldUtils.writeField(GravitinoEnv.getInstance(), "lockManager", new LockManager(config), true);
    FieldUtils.writeField(
        GravitinoEnv.getInstance(), "metalakeDispatcher", metalakeDispatcher, true);

    AuditInfo audit = AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build();
    BaseMetalake metalake =
        BaseMetalake.builder()
            .withId(idGenerator.nextId())
            .withName(METALAKE)
            .withVersion(SchemaVersion.V_0_1)
            .withComment("Test metalake")
            .withAuditInfo(audit)
            .build();
    entityStore.put(metalake, false /* overwritten */);
    when(metalakeDispatcher.metalakeExists(any())).thenReturn(true);
  }

  private static Config mockConfig() {
    Config config = Mockito.mock(Config.class);

    Mockito.when(config.get(ENTITY_STORE)).thenReturn(RELATIONAL_ENTITY_STORE);
    Mockito.when(config.get(ENTITY_RELATIONAL_STORE)).thenReturn(DEFAULT_ENTITY_RELATIONAL_STORE);
    Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_URL))
        .thenReturn(String.format("jdbc:h2:file:%s;DB_CLOSE_DELAY=-1;MODE=MYSQL", DB_DIR));
    Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER)).thenReturn("org.h2.Driver");
    Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_MAX_CONNECTIONS)).thenReturn(100);
    Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_WAIT_MILLISECONDS)).thenReturn(1000L);
    Mockito.when(config.get(STORE_TRANSACTION_MAX_SKEW_TIME)).thenReturn(1000L);
    Mockito.when(config.get(STORE_DELETE_AFTER_TIME)).thenReturn(20 * 60 * 1000L);
    Mockito.when(config.get(VERSION_RETENTION_COUNT)).thenReturn(1L);
    // Fix cache config for test
    Mockito.when(config.get(Configs.CACHE_ENABLED)).thenReturn(true);
    Mockito.when(config.get(Configs.CACHE_MAX_ENTRIES)).thenReturn(10_000);
    Mockito.when(config.get(Configs.CACHE_EXPIRATION_TIME)).thenReturn(3_600_000L);
    Mockito.when(config.get(Configs.CACHE_WEIGHER_ENABLED)).thenReturn(true);
    Mockito.when(config.get(Configs.CACHE_STATS_ENABLED)).thenReturn(false);
    Mockito.when(config.get(Configs.CACHE_IMPLEMENTATION)).thenReturn("caffeine");

    Mockito.doReturn(100000L).when(config).get(TREE_LOCK_MAX_NODE_IN_MEMORY);
    Mockito.doReturn(1000L).when(config).get(TREE_LOCK_MIN_NODE_IN_MEMORY);
    Mockito.doReturn(36000L).when(config).get(TREE_LOCK_CLEAN_INTERVAL);
    return config;
  }

  @AfterAll
  public static void tearDown() throws IOException {
    FileUtils.deleteDirectory(new File(JDBC_STORE_PATH));
  }

  @AfterEach
  public void cleanUp() {
    Arrays.stream(policyManager.listPolicies(METALAKE))
        .forEach(policyName -> policyManager.deletePolicy(METALAKE, policyName));
  }

  @Test
  public void testCreateAndGetPolicy() {
    String policyName = "policy_" + UUID.randomUUID().toString().replace("-", "");
    Map<String, Object> customRules = ImmutableMap.of("rule1", 1, "rule2", "value2");
    PolicyContent content = PolicyContents.custom(customRules, null);
    Policy policy = createCustomPolicy(METALAKE, policyName, content);

    Assertions.assertEquals(policyName, policy.name());
    Assertions.assertEquals("test", policy.policyType());
    Assertions.assertNull(policy.comment());
    Assertions.assertTrue(policy.enabled());
    Assertions.assertTrue(policy.exclusive());
    Assertions.assertTrue(policy.inheritable());
    Assertions.assertEquals(SUPPORTS_ALL_OBJECT_TYPES, policy.supportedObjectTypes());
    Assertions.assertNotNull(policy.content());
    Assertions.assertEquals(content, policy.content());

    Policy policy1 = policyManager.getPolicy(METALAKE, policyName);
    Assertions.assertEquals(policy, policy1);

    // Create a policy in non-existent metalake
    Exception e =
        Assertions.assertThrows(
            NoSuchMetalakeException.class,
            () -> createCustomPolicy("non_existent_metalake", policyName, content));
    Assertions.assertEquals("Metalake non_existent_metalake does not exist", e.getMessage());

    // create a policy with non-existent built-in type
    e =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                policyManager.createPolicy(
                    METALAKE,
                    policyName,
                    BUILT_IN_TYPE_PREFIX + "abc",
                    null,
                    true,
                    PolicyContents.custom(null, null)));
    Assertions.assertEquals(
        "Unknown built-in policy type: " + BUILT_IN_TYPE_PREFIX + "abc", e.getMessage());

    // Create an existent policy
    e =
        Assertions.assertThrows(
            PolicyAlreadyExistsException.class,
            () -> createCustomPolicy(METALAKE, policyName, content));
    Assertions.assertEquals(
        "Policy with name "
            + policyName
            + " under metalake metalake_for_policy_test already exists",
        e.getMessage());

    // Get a non-existent policy
    e =
        Assertions.assertThrows(
            NoSuchPolicyException.class,
            () -> policyManager.getPolicy(METALAKE, "non_existent_policy"));
    Assertions.assertEquals(
        "Policy with name non_existent_policy under metalake metalake_for_policy_test does not exist",
        e.getMessage());
  }

  @Test
  public void testCreateAndListPolicies() {
    String policyName1 = "policy1" + UUID.randomUUID().toString().replace("-", "");
    String policyName2 = "policy2" + UUID.randomUUID().toString().replace("-", "");
    String policyName3 = "policy3" + UUID.randomUUID().toString().replace("-", "");
    Map<String, Object> customRules = ImmutableMap.of("rule1", 1, "rule2", "value2");
    PolicyContent content = PolicyContents.custom(customRules, null);
    Set<String> expectedPolicyNames = ImmutableSet.of(policyName1, policyName2, policyName3);
    expectedPolicyNames.forEach(policyName -> createCustomPolicy(METALAKE, policyName, content));

    Set<String> policyNames =
        Arrays.stream(policyManager.listPolicies(METALAKE)).collect(Collectors.toSet());
    Assertions.assertEquals(3, policyNames.size());
    Assertions.assertEquals(expectedPolicyNames, policyNames);

    // list policies in non-existent metalake
    Exception e =
        Assertions.assertThrows(
            NoSuchMetalakeException.class,
            () -> policyManager.listPolicies("non_existent_metalake"));
    Assertions.assertEquals("Metalake non_existent_metalake does not exist", e.getMessage());
  }

  @Test
  public void testAlterPolicy() {
    String policyName = "policy1" + UUID.randomUUID().toString().replace("-", "");
    Map<String, Object> customRules = ImmutableMap.of("rule1", 1, "rule2", "value2");
    PolicyContent content = PolicyContents.custom(customRules, null);
    createCustomPolicy(METALAKE, policyName, content);

    // test rename policy
    String newName = "new_policy1" + UUID.randomUUID().toString().replace("-", "");
    PolicyChange rename = PolicyChange.rename(newName);
    Policy renamedPolicy = policyManager.alterPolicy(METALAKE, policyName, rename);

    Assertions.assertEquals(newName, renamedPolicy.name());
    Assertions.assertEquals("test", renamedPolicy.policyType());
    Assertions.assertNull(renamedPolicy.comment());
    Assertions.assertTrue(renamedPolicy.enabled());
    Assertions.assertTrue(renamedPolicy.exclusive());
    Assertions.assertTrue(renamedPolicy.inheritable());
    Assertions.assertEquals(SUPPORTS_ALL_OBJECT_TYPES, renamedPolicy.supportedObjectTypes());
    Assertions.assertEquals(content, renamedPolicy.content());

    // test change comment
    PolicyChange commentChange = PolicyChange.updateComment("new comment");
    Policy changedCommentPolicy =
        policyManager.alterPolicy(METALAKE, renamedPolicy.name(), commentChange);
    Assertions.assertEquals("new comment", changedCommentPolicy.comment());

    // test update content
    Map<String, Object> newCustomRules = ImmutableMap.of("rule3", 1, "rule4", "value2");
    PolicyContent newContent = PolicyContents.custom(newCustomRules, null);
    PolicyChange contentChange = PolicyChange.updateContent(newContent);
    Policy updatedContentPolicy =
        policyManager.alterPolicy(METALAKE, changedCommentPolicy.name(), contentChange);
    Assertions.assertEquals(newContent, updatedContentPolicy.content());

    // test disable policy
    Assertions.assertDoesNotThrow(
        () -> policyManager.disablePolicy(METALAKE, renamedPolicy.name()));
    Policy disabledPolicy = policyManager.getPolicy(METALAKE, renamedPolicy.name());
    Assertions.assertFalse(disabledPolicy.enabled());

    // test enable policy
    Assertions.assertDoesNotThrow(() -> policyManager.enablePolicy(METALAKE, renamedPolicy.name()));
    Policy enabledPolicy = policyManager.getPolicy(METALAKE, renamedPolicy.name());
    Assertions.assertTrue(enabledPolicy.enabled());
  }

  @Test
  public void testDeletePolicy() {
    String policyName = "policy1" + UUID.randomUUID().toString().replace("-", "");
    Map<String, Object> customRules = ImmutableMap.of("rule1", 1, "rule2", "value2");
    PolicyContent content = PolicyContents.custom(customRules, null);
    createCustomPolicy(METALAKE, policyName, content);

    // delete the policy
    Assertions.assertTrue(() -> policyManager.deletePolicy(METALAKE, policyName));

    // verify the policy is deleted
    Exception e =
        Assertions.assertThrows(
            NoSuchPolicyException.class, () -> policyManager.getPolicy(METALAKE, policyName));
    Assertions.assertEquals(
        "Policy with name "
            + policyName
            + " under metalake metalake_for_policy_test does not exist",
        e.getMessage());

    // delete a non-existent policy
    Assertions.assertFalse(() -> policyManager.deletePolicy(METALAKE, "non_existent_policy"));
  }

  private Policy createCustomPolicy(
      String metalakeName, String policyName, PolicyContent policyContent) {
    return policyManager.createPolicy(
        metalakeName,
        policyName,
        "test",
        null,
        true,
        true,
        true,
        SUPPORTS_ALL_OBJECT_TYPES,
        policyContent);
  }
}
