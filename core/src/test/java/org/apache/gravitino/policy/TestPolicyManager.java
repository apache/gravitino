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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
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
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.EntityStoreFactory;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.catalog.CatalogDispatcher;
import org.apache.gravitino.catalog.SchemaDispatcher;
import org.apache.gravitino.catalog.TableDispatcher;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchPolicyException;
import org.apache.gravitino.exceptions.NotFoundException;
import org.apache.gravitino.exceptions.PolicyAlreadyAssociatedException;
import org.apache.gravitino.exceptions.PolicyAlreadyExistsException;
import org.apache.gravitino.lock.LockManager;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.ColumnEntity;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.SchemaVersion;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.metalake.MetalakeDispatcher;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestPolicyManager {
  private static final String METALAKE = "metalake_for_policy_test";
  private static final String CATALOG = "catalog_for_policy_test";
  private static final String SCHEMA = "schema_for_policy_test";
  private static final String TABLE = "table_for_policy_test";
  private static final String COLUMN = "column_for_policy_test";
  private static final MetalakeDispatcher metalakeDispatcher = mock(MetalakeDispatcher.class);
  private static final CatalogDispatcher catalogDispatcher = mock(CatalogDispatcher.class);
  private static final SchemaDispatcher schemaDispatcher = mock(SchemaDispatcher.class);
  private static final TableDispatcher tableDispatcher = mock(TableDispatcher.class);
  private static final String JDBC_STORE_PATH =
      "/tmp/gravitino_jdbc_entityStore_" + UUID.randomUUID().toString().replace("-", "");
  private static final String DB_DIR = JDBC_STORE_PATH + "/testdb";
  private static final Set<MetadataObject.Type> SUPPORTS_OBJECT_TYPES =
      ImmutableSet.of(
          MetadataObject.Type.CATALOG, MetadataObject.Type.SCHEMA, MetadataObject.Type.TABLE);

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
    FieldUtils.writeField(GravitinoEnv.getInstance(), "catalogDispatcher", catalogDispatcher, true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "schemaDispatcher", schemaDispatcher, true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "tableDispatcher", tableDispatcher, true);

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

    CatalogEntity catalog =
        CatalogEntity.builder()
            .withId(idGenerator.nextId())
            .withName(CATALOG)
            .withNamespace(Namespace.of(METALAKE))
            .withType(Catalog.Type.RELATIONAL)
            .withProvider("test")
            .withComment("Test catalog")
            .withAuditInfo(audit)
            .build();
    entityStore.put(catalog, false /* overwritten */);
    when(catalogDispatcher.catalogExists(any())).thenReturn(true);

    SchemaEntity schema =
        SchemaEntity.builder()
            .withId(idGenerator.nextId())
            .withName(SCHEMA)
            .withNamespace(Namespace.of(METALAKE, CATALOG))
            .withComment("Test schema")
            .withAuditInfo(audit)
            .build();
    entityStore.put(schema, false /* overwritten */);
    when(schemaDispatcher.schemaExists(any())).thenReturn(true);

    ColumnEntity column =
        ColumnEntity.builder()
            .withId(idGenerator.nextId())
            .withName(COLUMN)
            .withPosition(0)
            .withComment("Test column")
            .withDataType(Types.IntegerType.get())
            .withNullable(true)
            .withAutoIncrement(false)
            .withAuditInfo(audit)
            .build();

    TableEntity table =
        TableEntity.builder()
            .withId(idGenerator.nextId())
            .withName(TABLE)
            .withColumns(Lists.newArrayList(column))
            .withNamespace(Namespace.of(METALAKE, CATALOG, SCHEMA))
            .withAuditInfo(audit)
            .build();
    entityStore.put(table, false /* overwritten */);
    when(tableDispatcher.tableExists(any())).thenReturn(true);
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
    Mockito.when(config.get(Configs.CACHE_LOCK_SEGMENTS)).thenReturn(16);

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
    PolicyContent content = PolicyContents.custom(customRules, SUPPORTS_OBJECT_TYPES, null);
    PolicyEntity policy = createCustomPolicy(METALAKE, policyName, content);

    Assertions.assertEquals(policyName, policy.name());
    Assertions.assertEquals(Policy.BuiltInType.CUSTOM, policy.policyType());
    Assertions.assertNull(policy.comment());
    Assertions.assertTrue(policy.enabled());
    Assertions.assertNotNull(policy.content());
    Assertions.assertEquals(content, policy.content());

    PolicyEntity policy1 = policyManager.getPolicy(METALAKE, policyName);
    Assertions.assertEquals(policy, policy1);

    // Create a policy in non-existent metalake
    Exception e =
        Assertions.assertThrows(
            NoSuchMetalakeException.class,
            () -> createCustomPolicy("non_existent_metalake", policyName, content));
    Assertions.assertEquals("Metalake non_existent_metalake does not exist", e.getMessage());

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
    PolicyContent content = PolicyContents.custom(customRules, SUPPORTS_OBJECT_TYPES, null);
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
    PolicyContent content = PolicyContents.custom(customRules, SUPPORTS_OBJECT_TYPES, null);
    createCustomPolicy(METALAKE, policyName, content);

    // test rename policy
    String newName = "new_policy1" + UUID.randomUUID().toString().replace("-", "");
    PolicyChange rename = PolicyChange.rename(newName);
    PolicyEntity renamedPolicy = policyManager.alterPolicy(METALAKE, policyName, rename);

    Assertions.assertEquals(newName, renamedPolicy.name());
    Assertions.assertEquals(Policy.BuiltInType.CUSTOM, renamedPolicy.policyType());
    Assertions.assertNull(renamedPolicy.comment());
    Assertions.assertTrue(renamedPolicy.enabled());
    Assertions.assertEquals(content, renamedPolicy.content());

    // test change comment
    PolicyChange commentChange = PolicyChange.updateComment("new comment");
    PolicyEntity changedCommentPolicy =
        policyManager.alterPolicy(METALAKE, renamedPolicy.name(), commentChange);
    Assertions.assertEquals("new comment", changedCommentPolicy.comment());

    // test update content
    Map<String, Object> newCustomRules = ImmutableMap.of("rule3", 1, "rule4", "value2");
    PolicyContent newContent = PolicyContents.custom(newCustomRules, SUPPORTS_OBJECT_TYPES, null);
    PolicyChange contentChange = PolicyChange.updateContent("custom", newContent);
    PolicyEntity updatedContentPolicy =
        policyManager.alterPolicy(METALAKE, changedCommentPolicy.name(), contentChange);
    Assertions.assertEquals(newContent, updatedContentPolicy.content());

    // test wrong policy type
    PolicyChange typeChange = PolicyChange.updateContent("wrong_type", newContent);
    Exception e =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> policyManager.alterPolicy(METALAKE, updatedContentPolicy.name(), typeChange));
    Assertions.assertEquals(
        "Unknown policy type: wrong_type, it should start with 'system_' or be 'custom'",
        e.getMessage());

    // test disable policy
    Assertions.assertDoesNotThrow(
        () -> policyManager.disablePolicy(METALAKE, renamedPolicy.name()));
    PolicyEntity disabledPolicy = policyManager.getPolicy(METALAKE, renamedPolicy.name());
    Assertions.assertFalse(disabledPolicy.enabled());

    // test enable policy
    Assertions.assertDoesNotThrow(() -> policyManager.enablePolicy(METALAKE, renamedPolicy.name()));
    PolicyEntity enabledPolicy = policyManager.getPolicy(METALAKE, renamedPolicy.name());
    Assertions.assertTrue(enabledPolicy.enabled());
  }

  @Test
  public void testDeletePolicy() {
    String policyName = "policy1" + UUID.randomUUID().toString().replace("-", "");
    Map<String, Object> customRules = ImmutableMap.of("rule1", 1, "rule2", "value2");
    PolicyContent content = PolicyContents.custom(customRules, SUPPORTS_OBJECT_TYPES, null);
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

  @Test
  public void testAssociatePoliciesForMetadataObject() {
    Map<String, Object> customRules = ImmutableMap.of("rule1", 1, "rule2", "value2");
    PolicyContent content = PolicyContents.custom(customRules, SUPPORTS_OBJECT_TYPES, null);
    String policyName1 = "policy1" + UUID.randomUUID().toString().replace("-", "");
    createCustomPolicy(METALAKE, policyName1, content);
    String policyName2 = "policy2" + UUID.randomUUID().toString().replace("-", "");
    createCustomPolicy(METALAKE, policyName2, content);
    String policyName3 = "policy3" + UUID.randomUUID().toString().replace("-", "");
    createCustomPolicy(METALAKE, policyName3, content);

    // Test associate policies for catalog
    MetadataObject catalogObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofCatalog(METALAKE, CATALOG), Entity.EntityType.CATALOG);
    String[] policiesToAdd = new String[] {policyName1, policyName2, policyName3};

    String[] policies =
        policyManager.associatePoliciesForMetadataObject(
            METALAKE, catalogObject, policiesToAdd, null);

    Assertions.assertEquals(3, policies.length);
    Assertions.assertEquals(
        ImmutableSet.of(policyName1, policyName2, policyName3), ImmutableSet.copyOf(policies));

    // Test disassociate policies for catalog
    String[] policiesToRemove = new String[] {policyName1};
    String[] policies1 =
        policyManager.associatePoliciesForMetadataObject(
            METALAKE, catalogObject, null, policiesToRemove);

    Assertions.assertEquals(2, policies1.length);
    Assertions.assertEquals(
        ImmutableSet.of(policyName2, policyName3), ImmutableSet.copyOf(policies1));

    // Test associate and disassociate no policies for catalog
    String[] policies2 =
        policyManager.associatePoliciesForMetadataObject(METALAKE, catalogObject, null, null);

    Assertions.assertEquals(2, policies2.length);
    Assertions.assertEquals(
        ImmutableSet.of(policyName2, policyName3), ImmutableSet.copyOf(policies2));

    // Test re-associate policies for catalog
    Throwable e =
        Assertions.assertThrows(
            PolicyAlreadyAssociatedException.class,
            () ->
                policyManager.associatePoliciesForMetadataObject(
                    METALAKE, catalogObject, policiesToAdd, null));
    Assertions.assertTrue(
        e.getMessage().contains("Failed to associate policies for metadata object"));

    // Test associate and disassociate non-existent policies for catalog
    String[] policies3 =
        policyManager.associatePoliciesForMetadataObject(
            METALAKE, catalogObject, new String[] {"policy4", "policy5"}, new String[] {"policy6"});

    Assertions.assertEquals(2, policies3.length);
    Assertions.assertEquals(
        ImmutableSet.of(policyName2, policyName3), ImmutableSet.copyOf(policies3));

    // Test associate policies for non-existent metadata object
    MetadataObject nonExistentObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofCatalog(METALAKE, "non_existent_catalog"),
            Entity.EntityType.CATALOG);
    Throwable e1 =
        Assertions.assertThrows(
            NotFoundException.class,
            () ->
                policyManager.associatePoliciesForMetadataObject(
                    METALAKE, nonExistentObject, policiesToAdd, null));
    Assertions.assertTrue(
        e1.getMessage().contains("Failed to associate policies for metadata object"));

    // Test associate policies for unsupported metadata object
    MetadataObject metalakeObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofMetalake(METALAKE), Entity.EntityType.METALAKE);
    Throwable e2 =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                policyManager.associatePoliciesForMetadataObject(
                    METALAKE, metalakeObject, policiesToAdd, null));
    Assertions.assertTrue(
        e2.getMessage().contains("Cannot associate policies for unsupported metadata object type"),
        "Actual message: " + e2.getMessage());

    Assertions.assertTrue(
        e2.getMessage().contains("Cannot associate policies for unsupported metadata object type"),
        "Actual message: " + e2.getMessage());

    // Test associate policies for schema
    MetadataObject schemaObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofSchema(METALAKE, CATALOG, SCHEMA), Entity.EntityType.SCHEMA);
    String[] policies4 =
        policyManager.associatePoliciesForMetadataObject(
            METALAKE, schemaObject, policiesToAdd, null);

    Assertions.assertEquals(3, policies4.length);
    Assertions.assertEquals(
        ImmutableSet.of(policyName1, policyName2, policyName3), ImmutableSet.copyOf(policies4));

    // Test associate policies for table
    String[] policiesToAdd1 = new String[] {policyName1};
    MetadataObject tableObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofTable(METALAKE, CATALOG, SCHEMA, TABLE), Entity.EntityType.TABLE);
    String[] policies5 =
        policyManager.associatePoliciesForMetadataObject(
            METALAKE, tableObject, policiesToAdd1, null);

    Assertions.assertEquals(1, policies5.length);
    Assertions.assertEquals(ImmutableSet.of(policyName1), ImmutableSet.copyOf(policies5));

    // Test associate and disassociate same policies for table
    String[] policiesToAdd2 = new String[] {policyName2, policyName3};
    String[] policiesToRemove1 = new String[] {policyName2};
    String[] policies6 =
        policyManager.associatePoliciesForMetadataObject(
            METALAKE, tableObject, policiesToAdd2, policiesToRemove1);

    Assertions.assertEquals(2, policies6.length);
    Assertions.assertEquals(
        ImmutableSet.of(policyName1, policyName3), ImmutableSet.copyOf(policies6));
  }

  @Test
  public void testListMetadataObjectsForPolicy() {
    Map<String, Object> customRules = ImmutableMap.of("rule1", 1, "rule2", "value2");
    PolicyContent content = PolicyContents.custom(customRules, SUPPORTS_OBJECT_TYPES, null);
    String policyName1 = "policy1" + UUID.randomUUID().toString().replace("-", "");
    createCustomPolicy(METALAKE, policyName1, content);
    String policyName2 = "policy2" + UUID.randomUUID().toString().replace("-", "");
    createCustomPolicy(METALAKE, policyName2, content);
    String policyName3 = "policy3" + UUID.randomUUID().toString().replace("-", "");
    createCustomPolicy(METALAKE, policyName3, content);

    MetadataObject catalogObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofCatalog(METALAKE, CATALOG), Entity.EntityType.CATALOG);
    MetadataObject schemaObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofSchema(METALAKE, CATALOG, SCHEMA), Entity.EntityType.SCHEMA);
    MetadataObject tableObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofTable(METALAKE, CATALOG, SCHEMA, TABLE), Entity.EntityType.TABLE);

    policyManager.associatePoliciesForMetadataObject(
        METALAKE, catalogObject, new String[] {policyName1, policyName2, policyName3}, null);
    policyManager.associatePoliciesForMetadataObject(
        METALAKE, schemaObject, new String[] {policyName1, policyName2}, null);
    policyManager.associatePoliciesForMetadataObject(
        METALAKE, tableObject, new String[] {policyName1}, null);

    MetadataObject[] objects = policyManager.listMetadataObjectsForPolicy(METALAKE, policyName1);
    Assertions.assertEquals(3, objects.length);
    Assertions.assertEquals(
        ImmutableSet.of(catalogObject, schemaObject, tableObject), ImmutableSet.copyOf(objects));

    MetadataObject[] objects1 = policyManager.listMetadataObjectsForPolicy(METALAKE, policyName2);
    Assertions.assertEquals(2, objects1.length);
    Assertions.assertEquals(
        ImmutableSet.of(catalogObject, schemaObject), ImmutableSet.copyOf(objects1));

    MetadataObject[] objects2 = policyManager.listMetadataObjectsForPolicy(METALAKE, policyName3);
    Assertions.assertEquals(1, objects2.length);
    Assertions.assertEquals(ImmutableSet.of(catalogObject), ImmutableSet.copyOf(objects2));

    // List metadata objects for non-existent policy
    Throwable e =
        Assertions.assertThrows(
            NoSuchPolicyException.class,
            () -> policyManager.listMetadataObjectsForPolicy(METALAKE, "non_existent_policy"));
    Assertions.assertTrue(
        e.getMessage()
            .contains(
                "Policy with name non_existent_policy under metalake "
                    + METALAKE
                    + " does not exist"));
  }

  @Test
  public void testListPoliciesForMetadataObject() {
    Map<String, Object> customRules = ImmutableMap.of("rule1", 1, "rule2", "value2");
    PolicyContent content = PolicyContents.custom(customRules, SUPPORTS_OBJECT_TYPES, null);
    String policyName1 = "policy1" + UUID.randomUUID().toString().replace("-", "");
    PolicyEntity policy1 = createCustomPolicy(METALAKE, policyName1, content);
    String policyName2 = "policy2" + UUID.randomUUID().toString().replace("-", "");
    PolicyEntity policy2 = createCustomPolicy(METALAKE, policyName2, content);
    String policyName3 = "policy3" + UUID.randomUUID().toString().replace("-", "");
    PolicyEntity policy3 = createCustomPolicy(METALAKE, policyName3, content);

    MetadataObject catalogObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofCatalog(METALAKE, CATALOG), Entity.EntityType.CATALOG);
    MetadataObject schemaObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofSchema(METALAKE, CATALOG, SCHEMA), Entity.EntityType.SCHEMA);
    MetadataObject tableObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofTable(METALAKE, CATALOG, SCHEMA, TABLE), Entity.EntityType.TABLE);

    policyManager.associatePoliciesForMetadataObject(
        METALAKE,
        catalogObject,
        new String[] {policy1.name(), policy2.name(), policy3.name()},
        null);
    policyManager.associatePoliciesForMetadataObject(
        METALAKE, schemaObject, new String[] {policy1.name(), policy2.name()}, null);
    policyManager.associatePoliciesForMetadataObject(
        METALAKE, tableObject, new String[] {policy1.name()}, null);

    String[] policies = policyManager.listPoliciesForMetadataObject(METALAKE, catalogObject);
    Assertions.assertEquals(3, policies.length);
    Assertions.assertEquals(
        ImmutableSet.of(policyName1, policyName2, policyName3), ImmutableSet.copyOf(policies));

    PolicyEntity[] policiesInfo =
        policyManager.listPolicyInfosForMetadataObject(METALAKE, catalogObject);
    Assertions.assertEquals(3, policiesInfo.length);
    Assertions.assertEquals(
        ImmutableSet.of(policy1, policy2, policy3), ImmutableSet.copyOf(policiesInfo));

    String[] policies1 = policyManager.listPoliciesForMetadataObject(METALAKE, schemaObject);
    Assertions.assertEquals(2, policies1.length);
    Assertions.assertEquals(
        ImmutableSet.of(policyName1, policyName2), ImmutableSet.copyOf(policies1));

    PolicyEntity[] policiesInfo1 =
        policyManager.listPolicyInfosForMetadataObject(METALAKE, schemaObject);
    Assertions.assertEquals(2, policiesInfo1.length);
    Assertions.assertEquals(ImmutableSet.of(policy1, policy2), ImmutableSet.copyOf(policiesInfo1));

    String[] policies2 = policyManager.listPoliciesForMetadataObject(METALAKE, tableObject);
    Assertions.assertEquals(1, policies2.length);
    Assertions.assertEquals(ImmutableSet.of(policyName1), ImmutableSet.copyOf(policies2));

    PolicyEntity[] policiesInfo2 =
        policyManager.listPolicyInfosForMetadataObject(METALAKE, tableObject);
    Assertions.assertEquals(1, policiesInfo2.length);
    Assertions.assertEquals(ImmutableSet.of(policy1), ImmutableSet.copyOf(policiesInfo2));

    // List policies for non-existent metadata object
    MetadataObject nonExistentObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofCatalog(METALAKE, "non_existent_catalog"),
            Entity.EntityType.CATALOG);
    Throwable e =
        Assertions.assertThrows(
            NotFoundException.class,
            () -> policyManager.listPoliciesForMetadataObject(METALAKE, nonExistentObject));
    Assertions.assertTrue(
        e.getMessage()
            .contains("Failed to list policies for metadata object " + nonExistentObject));
  }

  @Test
  public void testGetPolicyForMetadataObject() {
    Map<String, Object> customRules = ImmutableMap.of("rule1", 1, "rule2", "value2");
    PolicyContent content = PolicyContents.custom(customRules, SUPPORTS_OBJECT_TYPES, null);
    String policyName1 = "policy1" + UUID.randomUUID().toString().replace("-", "");
    PolicyEntity policy1 = createCustomPolicy(METALAKE, policyName1, content);
    String policyName2 = "policy2" + UUID.randomUUID().toString().replace("-", "");
    PolicyEntity policy2 = createCustomPolicy(METALAKE, policyName2, content);
    String policyName3 = "policy3" + UUID.randomUUID().toString().replace("-", "");
    PolicyEntity policy3 = createCustomPolicy(METALAKE, policyName3, content);

    MetadataObject catalogObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofCatalog(METALAKE, CATALOG), Entity.EntityType.CATALOG);
    MetadataObject schemaObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofSchema(METALAKE, CATALOG, SCHEMA), Entity.EntityType.SCHEMA);
    MetadataObject tableObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofTable(METALAKE, CATALOG, SCHEMA, TABLE), Entity.EntityType.TABLE);

    policyManager.associatePoliciesForMetadataObject(
        METALAKE, catalogObject, new String[] {policyName1, policyName2, policyName3}, null);
    policyManager.associatePoliciesForMetadataObject(
        METALAKE, schemaObject, new String[] {policyName1, policyName2}, null);
    policyManager.associatePoliciesForMetadataObject(
        METALAKE, tableObject, new String[] {policyName1}, null);

    PolicyEntity result =
        policyManager.getPolicyForMetadataObject(METALAKE, catalogObject, policyName1);
    Assertions.assertEquals(policy1, result);

    PolicyEntity result1 =
        policyManager.getPolicyForMetadataObject(METALAKE, schemaObject, policyName1);
    Assertions.assertEquals(policy1, result1);

    PolicyEntity result2 =
        policyManager.getPolicyForMetadataObject(METALAKE, schemaObject, policy2.name());
    Assertions.assertEquals(policy2, result2);

    PolicyEntity result3 =
        policyManager.getPolicyForMetadataObject(METALAKE, catalogObject, policy3.name());
    Assertions.assertEquals(policy3, result3);

    PolicyEntity result4 =
        policyManager.getPolicyForMetadataObject(METALAKE, tableObject, policy1.name());
    Assertions.assertEquals(policy1, result4);

    // Test get non-existent policy for metadata object
    Throwable e =
        Assertions.assertThrows(
            NoSuchPolicyException.class,
            () ->
                policyManager.getPolicyForMetadataObject(
                    METALAKE, catalogObject, "non_existent_policy"));
    Assertions.assertTrue(e.getMessage().contains("Policy non_existent_policy does not exist"));

    Throwable e1 =
        Assertions.assertThrows(
            NoSuchPolicyException.class,
            () -> policyManager.getPolicyForMetadataObject(METALAKE, schemaObject, policy3.name()));
    Assertions.assertTrue(
        e1.getMessage().contains("Policy " + policyName3 + " does not exist"),
        "Actual message: " + e1.getMessage());

    Throwable e2 =
        Assertions.assertThrows(
            NoSuchPolicyException.class,
            () -> policyManager.getPolicyForMetadataObject(METALAKE, tableObject, policy2.name()));
    Assertions.assertTrue(
        e2.getMessage().contains("Policy " + policyName2 + " does not exist"),
        "Actual message: " + e2.getMessage());

    // Test get policy for non-existent metadata object
    MetadataObject nonExistentObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofCatalog(METALAKE, "non_existent_catalog"),
            Entity.EntityType.CATALOG);
    Throwable e3 =
        Assertions.assertThrows(
            NotFoundException.class,
            () ->
                policyManager.getPolicyForMetadataObject(
                    METALAKE, nonExistentObject, policy1.name()));
    Assertions.assertTrue(
        e3.getMessage().contains("Failed to get policy for metadata object " + nonExistentObject));
  }

  private PolicyEntity createCustomPolicy(
      String metalakeName, String policyName, PolicyContent policyContent) {
    return policyManager.createPolicy(
        metalakeName, policyName, Policy.BuiltInType.CUSTOM, null, true, policyContent);
  }
}
