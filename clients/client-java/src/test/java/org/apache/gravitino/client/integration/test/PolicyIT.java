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
package org.apache.gravitino.client.integration.test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Schema;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.dto.tag.MetadataObjectDTO;
import org.apache.gravitino.exceptions.NoSuchPolicyException;
import org.apache.gravitino.exceptions.PolicyAlreadyAssociatedException;
import org.apache.gravitino.exceptions.PolicyAlreadyExistsException;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.container.HiveContainer;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.model.Model;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyChange;
import org.apache.gravitino.policy.PolicyContent;
import org.apache.gravitino.policy.PolicyContents;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("gravitino-docker-test")
public class PolicyIT extends BaseIT {

  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();

  private static final String metalakeName = GravitinoITUtils.genRandomName("policy_it_metalake");

  private static GravitinoMetalake metalake;
  private static Catalog relationalCatalog;
  private static Schema schema;
  private static Table table;

  private static Catalog modelCatalog;
  private static Schema modelSchema;
  private static Model model;

  @BeforeAll
  public void setUp() {
    containerSuite.startHiveContainer();
    String hmsUri =
        String.format(
            "thrift://%s:%d",
            containerSuite.getHiveContainer().getContainerIpAddress(),
            HiveContainer.HIVE_METASTORE_PORT);

    // Create metalake
    Assertions.assertFalse(client.metalakeExists(metalakeName));
    metalake = client.createMetalake(metalakeName, "metalake", Collections.emptyMap());

    // Create catalog
    String catalogName = GravitinoITUtils.genRandomName("policy_it_catalog");
    Assertions.assertFalse(metalake.catalogExists(catalogName));
    relationalCatalog =
        metalake.createCatalog(
            catalogName,
            Catalog.Type.RELATIONAL,
            "hive",
            "comment",
            ImmutableMap.of("metastore.uris", hmsUri));

    // Create schema
    String schemaName = GravitinoITUtils.genRandomName("policy_it_schema");
    Assertions.assertFalse(relationalCatalog.asSchemas().schemaExists(schemaName));
    schema =
        relationalCatalog.asSchemas().createSchema(schemaName, "comment", Collections.emptyMap());

    // Create table
    String tableName = GravitinoITUtils.genRandomName("policy_it_table");
    Assertions.assertFalse(
        relationalCatalog.asTableCatalog().tableExists(NameIdentifier.of(schemaName, tableName)));
    table =
        relationalCatalog
            .asTableCatalog()
            .createTable(
                NameIdentifier.of(schemaName, tableName),
                new Column[] {
                  Column.of("col1", Types.IntegerType.get()),
                  Column.of("col2", Types.StringType.get())
                },
                "comment",
                Collections.emptyMap());

    // Create model catalog
    String modelCatalogName = GravitinoITUtils.genRandomName("policy_it_model_catalog");
    Assertions.assertFalse(metalake.catalogExists(modelCatalogName));
    modelCatalog =
        metalake.createCatalog(
            modelCatalogName, Catalog.Type.MODEL, "comment", Collections.emptyMap());

    // Create model schema
    String modelSchemaName = GravitinoITUtils.genRandomName("policy_it_model_schema");
    Assertions.assertFalse(modelCatalog.asSchemas().schemaExists(modelSchemaName));
    modelSchema =
        modelCatalog.asSchemas().createSchema(modelSchemaName, "comment", Collections.emptyMap());

    // Create model
    String modelName = GravitinoITUtils.genRandomName("policy_it_model");
    Assertions.assertFalse(
        modelCatalog.asModelCatalog().modelExists(NameIdentifier.of(modelSchemaName, modelName)));
    model =
        modelCatalog
            .asModelCatalog()
            .registerModel(
                NameIdentifier.of(modelSchemaName, modelName), "comment", Collections.emptyMap());
  }

  @AfterAll
  public void tearDown() {
    relationalCatalog.asTableCatalog().dropTable(NameIdentifier.of(schema.name(), table.name()));
    relationalCatalog.asSchemas().dropSchema(schema.name(), true);
    metalake.dropCatalog(relationalCatalog.name(), true);

    modelCatalog.asModelCatalog().deleteModel(NameIdentifier.of(modelSchema.name(), model.name()));
    modelCatalog.asSchemas().dropSchema(modelSchema.name(), true);
    metalake.dropCatalog(modelCatalog.name(), true);

    client.dropMetalake(metalakeName, true);

    if (client != null) {
      client.close();
      client = null;
    }

    try {
      closer.close();
    } catch (Exception e) {
      // Swallow exceptions
    }
  }

  @AfterEach
  public void cleanUp() {
    String[] tablePolicies = table.supportsPolicies().listPolicies();
    table.supportsPolicies().associatePolicies(null, tablePolicies);

    String[] schemaPolicies = schema.supportsPolicies().listPolicies();
    schema.supportsPolicies().associatePolicies(null, schemaPolicies);

    String[] catalogPolicies = relationalCatalog.supportsPolicies().listPolicies();
    relationalCatalog.supportsPolicies().associatePolicies(null, catalogPolicies);

    String[] policies = metalake.listPolicies();
    for (String policy : policies) {
      metalake.deletePolicy(policy);
    }
  }

  @Test
  public void testCreateGetAndListPolicy() {
    String policyName = GravitinoITUtils.genRandomName("policy_it_policy");
    Assertions.assertThrows(NoSuchPolicyException.class, () -> metalake.getPolicy(policyName));

    // Test create
    PolicyContent content =
        PolicyContents.custom(
            ImmutableMap.of("rule1", "value1"), ImmutableSet.of(MetadataObject.Type.TABLE), null);
    Policy policy = metalake.createPolicy(policyName, "custom", "comment", true, content);
    Assertions.assertEquals(policyName, policy.name());
    Assertions.assertEquals("comment", policy.comment());
    Assertions.assertEquals("custom", policy.policyType());
    Assertions.assertTrue(policy.enabled());
    Assertions.assertFalse(policy.inherited().isPresent());
    Assertions.assertEquals(content, policy.content());

    // Test already existed policy
    Assertions.assertThrows(
        PolicyAlreadyExistsException.class,
        () -> metalake.createPolicy(policyName, "custom", "comment", true, content));

    // Test get
    Policy fetchedPolicy = metalake.getPolicy(policyName);
    Assertions.assertEquals(policy, fetchedPolicy);
    Assertions.assertEquals(policyName, fetchedPolicy.name());
    Assertions.assertEquals("comment", fetchedPolicy.comment());
    Assertions.assertEquals("custom", fetchedPolicy.policyType());
    Assertions.assertTrue(fetchedPolicy.enabled());
    Assertions.assertFalse(fetchedPolicy.inherited().isPresent());
    Assertions.assertEquals(content, fetchedPolicy.content());

    // test List names
    String policyName1 = GravitinoITUtils.genRandomName("policy_it_policy1");
    Policy policy1 = metalake.createPolicy(policyName1, "custom", null, false, content);
    Assertions.assertEquals(policyName1, policy1.name());

    String[] policyNames = metalake.listPolicies();
    Assertions.assertEquals(2, policyNames.length);
    Set<String> policyNamesSet = Sets.newHashSet(policyName, policyName1);
    Set<String> resultPolicyNamesSet = Sets.newHashSet(policyNames);
    Assertions.assertEquals(policyNamesSet, resultPolicyNamesSet);

    // test List policies
    Set<Policy> policies = Sets.newHashSet(metalake.listPolicyInfos());
    Set<Policy> expectedPolicies = Sets.newHashSet(policy, policy1);
    Assertions.assertEquals(expectedPolicies, policies);

    // Test null comment
    String policyName2 = GravitinoITUtils.genRandomName("policy_it_policy2");
    Policy policy2 = metalake.createPolicy(policyName2, "custom", null, true, content);

    Assertions.assertEquals(policyName2, policy2.name());
    Assertions.assertNull(policy2.comment());

    Policy LoadedPolicy2 = metalake.getPolicy(policyName2);
    Assertions.assertEquals(policy2, LoadedPolicy2);

    // Test null content
    String policyName3 = GravitinoITUtils.genRandomName("policy_it_policy3");
    Exception e =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> metalake.createPolicy(policyName3, "custom", null, true, null));
    Assertions.assertEquals("\"content\" is required and cannot be null", e.getMessage());

    // Test null supported types in content
    e =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                metalake.createPolicy(
                    policyName3, "custom", null, true, PolicyContents.custom(null, null, null)));
    Assertions.assertEquals("supportedObjectTypes cannot be empty", e.getMessage());

    // Test enable false
    String policyName4 = GravitinoITUtils.genRandomName("policy_it_policy4");
    Policy policy4 = metalake.createPolicy(policyName4, "custom", null, false, content);
    Assertions.assertEquals(policyName4, policy4.name());
    Assertions.assertFalse(policy4.enabled());
    Assertions.assertFalse(policy4.inherited().isPresent());

    Policy loadedPolicy4 = metalake.getPolicy(policyName4);
    Assertions.assertEquals(policy4, loadedPolicy4);
    Assertions.assertFalse(loadedPolicy4.enabled());

    // Test illegal policy type
    String policyName6 = GravitinoITUtils.genRandomName("policy_it_policy6");
    e =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> metalake.createPolicy(policyName6, "illegal_type", null, true, content));
    Assertions.assertTrue(e.getMessage().contains("Unknown policy type"));
  }

  @Test
  public void testCreateAndAlterPolicy() {
    PolicyContent content =
        PolicyContents.custom(
            ImmutableMap.of("rule1", "value1"), ImmutableSet.of(MetadataObject.Type.TABLE), null);
    String policyName = GravitinoITUtils.genRandomName("policy_it_policy");
    metalake.createPolicy(policyName, "custom", "comment", true, content);

    Policy[] policies = metalake.listPolicyInfos();
    Assertions.assertEquals(1, policies.length);
    Assertions.assertEquals(policyName, policies[0].name());
    Assertions.assertEquals("comment", policies[0].comment());
    Assertions.assertEquals("custom", policies[0].policyType());

    // Test rename and update comment
    String newPolicyName = GravitinoITUtils.genRandomName("policy_it_policy_new");
    PolicyChange rename = PolicyChange.rename(newPolicyName);
    PolicyChange updateComment = PolicyChange.updateComment("new comment");

    Policy alteredPolicy = metalake.alterPolicy(policyName, rename, updateComment);
    Assertions.assertEquals(newPolicyName, alteredPolicy.name());
    Assertions.assertEquals("new comment", alteredPolicy.comment());
    Assertions.assertFalse(alteredPolicy.inherited().isPresent());

    // Test update content
    PolicyContent newContent =
        PolicyContents.custom(
            ImmutableMap.of("rule2", "value2"),
            ImmutableSet.of(MetadataObject.Type.TABLE),
            ImmutableMap.of("key1", "value1"));
    PolicyChange updateContent = PolicyChange.updateContent("custom", newContent);

    Policy alteredPolicy2 = metalake.alterPolicy(newPolicyName, updateContent);
    Assertions.assertEquals(newPolicyName, alteredPolicy2.name());
    Assertions.assertEquals("new comment", alteredPolicy2.comment());
    Assertions.assertFalse(alteredPolicy2.inherited().isPresent());
    Assertions.assertEquals(newContent, alteredPolicy2.content());

    // Test list after alter
    policies = metalake.listPolicyInfos();
    Assertions.assertEquals(1, policies.length);
    Assertions.assertEquals(newPolicyName, policies[0].name());
    Assertions.assertEquals("new comment", policies[0].comment());
    Assertions.assertEquals(newContent, policies[0].content());
    Assertions.assertFalse(policies[0].inherited().isPresent());

    // Test update content with wrong type
    PolicyChange updateContentWrongType = PolicyChange.updateContent("wrong_type", newContent);

    Exception e =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> metalake.alterPolicy(newPolicyName, updateContentWrongType));
    Assertions.assertTrue(e.getMessage().contains("Unknown policy type"));

    // Test throw NoSuchPolicyException
    Assertions.assertThrows(
        NoSuchPolicyException.class, () -> metalake.alterPolicy("non-existed-policy", rename));

    // Test alter policy on no comment
    String policyName1 = GravitinoITUtils.genRandomName("policy_it_policy1");
    metalake.createPolicy(policyName1, "custom", null, true, content);

    String newPolicyName1 = GravitinoITUtils.genRandomName("policy_it_policy_new1");
    PolicyChange rename1 = PolicyChange.rename(newPolicyName1);
    PolicyChange updateComment1 = PolicyChange.updateComment("new comment1");
    PolicyChange updateContent1 = PolicyChange.updateContent("custom", newContent);

    Policy alteredPolicy5 =
        metalake.alterPolicy(policyName1, rename1, updateComment1, updateContent1);
    Assertions.assertEquals(newPolicyName1, alteredPolicy5.name());
    Assertions.assertEquals("new comment1", alteredPolicy5.comment());
    Assertions.assertEquals(newContent, alteredPolicy5.content());
    Assertions.assertFalse(alteredPolicy5.inherited().isPresent());

    // Test alter supported types in content
    PolicyContent newContent2 =
        PolicyContents.custom(
            ImmutableMap.of("rule3", "value3"),
            ImmutableSet.of(MetadataObject.Type.TABLE, MetadataObject.Type.MODEL),
            null);
    PolicyChange updateContent2 = PolicyChange.updateContent("custom", newContent2);
    Policy alteredPolicy6 = metalake.alterPolicy(newPolicyName1, updateContent2);

    Assertions.assertEquals(newPolicyName1, alteredPolicy6.name());
    Assertions.assertEquals(newContent2, alteredPolicy6.content());

    alteredPolicy6 = metalake.getPolicy(newPolicyName1);
    Assertions.assertEquals(newPolicyName1, alteredPolicy6.name());
    Assertions.assertEquals(newContent2, alteredPolicy6.content());

    // Test disable and enable
    Assertions.assertDoesNotThrow(() -> metalake.disablePolicy(newPolicyName));
    Policy policy = metalake.getPolicy(newPolicyName);
    Assertions.assertFalse(policy.enabled());

    Assertions.assertDoesNotThrow(() -> metalake.enablePolicy(newPolicyName));
    policy = metalake.getPolicy(policy.name());
    Assertions.assertTrue(policy.enabled());
  }

  @Test
  public void testCreateAndDeletePolicy() {
    PolicyContent content =
        PolicyContents.custom(
            ImmutableMap.of("rule1", "value1"), ImmutableSet.of(MetadataObject.Type.TABLE), null);
    String policyName = GravitinoITUtils.genRandomName("policy_it_policy");
    metalake.createPolicy(policyName, "custom", null, true, content);

    // Test delete
    Assertions.assertTrue(metalake.deletePolicy(policyName));
    Assertions.assertFalse(metalake.deletePolicy(policyName));
  }

  @Test
  public void testAssociatePoliciesToCatalog() {
    Policy policy1 =
        createCustomPolicy(GravitinoITUtils.genRandomName("policy_it_catalog_policy1"));
    Policy policy2 =
        createCustomPolicy(GravitinoITUtils.genRandomName("policy_it_catalog_policy2"));

    // Test associate policies to catalog
    String[] policies =
        relationalCatalog
            .supportsPolicies()
            .associatePolicies(new String[] {policy1.name(), policy2.name()}, null);

    Assertions.assertEquals(2, policies.length);
    Set<String> policyNames = Sets.newHashSet(policies);
    Assertions.assertTrue(policyNames.contains(policy1.name()));
    Assertions.assertTrue(policyNames.contains(policy2.name()));

    // Test disassociate policies from catalog
    String[] policies1 =
        relationalCatalog
            .supportsPolicies()
            .associatePolicies(null, new String[] {policy1.name(), policy2.name()});
    Assertions.assertEquals(0, policies1.length);

    // Test associate non-existed policies to catalog
    String[] policies2 =
        relationalCatalog
            .supportsPolicies()
            .associatePolicies(new String[] {"non-existed-policy"}, null);
    Assertions.assertEquals(0, policies2.length);

    // Test disassociate non-existed policies from catalog
    String[] policies3 =
        relationalCatalog
            .supportsPolicies()
            .associatePolicies(null, new String[] {"non-existed-policy"});
    Assertions.assertEquals(0, policies3.length);

    // Test associate same policies to catalog
    String[] policies4 =
        relationalCatalog
            .supportsPolicies()
            .associatePolicies(new String[] {policy1.name(), policy1.name()}, null);
    Assertions.assertEquals(1, policies4.length);
    Assertions.assertEquals(policy1.name(), policies4[0]);

    // Test associate same policy again to catalog
    Assertions.assertThrows(
        PolicyAlreadyAssociatedException.class,
        () ->
            relationalCatalog
                .supportsPolicies()
                .associatePolicies(new String[] {policy1.name()}, null));

    // Test associate and disassociate same policies to catalog
    String[] policies5 =
        relationalCatalog
            .supportsPolicies()
            .associatePolicies(new String[] {policy2.name()}, new String[] {policy2.name()});
    Assertions.assertEquals(1, policies5.length);
    Assertions.assertEquals(policy1.name(), policies5[0]);

    // Test List associated policies for catalog
    String[] policies6 = relationalCatalog.supportsPolicies().listPolicies();
    Assertions.assertEquals(1, policies6.length);
    Assertions.assertEquals(policy1.name(), policies6[0]);

    // Test List associated policies with details for catalog
    Policy[] policies7 = relationalCatalog.supportsPolicies().listPolicyInfos();
    Assertions.assertEquals(1, policies7.length);
    Assertions.assertEquals(policy1, policies7[0]);
    Assertions.assertFalse(policies7[0].inherited().get());
    Assertions.assertTrue(policies7[0].enabled());

    // Test disable the policy then list again
    Assertions.assertDoesNotThrow(() -> metalake.disablePolicy(policy1.name()));
    Policy[] policies8 = relationalCatalog.supportsPolicies().listPolicyInfos();
    Assertions.assertEquals(1, policies8.length);
    Assertions.assertEquals(policy1.name(), policies8[0].name());
    Assertions.assertFalse(policies8[0].enabled());
    Assertions.assertDoesNotThrow(() -> metalake.enablePolicy(policy1.name()));

    // Test get associated policy for catalog
    Policy policy = relationalCatalog.supportsPolicies().getPolicy(policy1.name());
    Assertions.assertEquals(policy1.enabled(), policy.enabled());
    Assertions.assertFalse(policy.inherited().get());

    // Test get non-existed policy for catalog
    Assertions.assertThrows(
        NoSuchPolicyException.class,
        () -> relationalCatalog.supportsPolicies().getPolicy("non-existed-policy"));

    // Test get objects associated with policy
    Assertions.assertEquals(1, policy.associatedObjects().count());
    MetadataObject catalogObject = policy.associatedObjects().objects()[0];
    Assertions.assertEquals(relationalCatalog.name(), catalogObject.name());
    Assertions.assertEquals(MetadataObject.Type.CATALOG, catalogObject.type());
  }

  @Test
  public void testAssociatePoliciesToSchema() {
    Policy policy1 = createCustomPolicy(GravitinoITUtils.genRandomName("policy_it_schema_policy1"));
    Policy policy2 = createCustomPolicy(GravitinoITUtils.genRandomName("policy_it_schema_policy2"));

    // Associate policies to catalog
    relationalCatalog.supportsPolicies().associatePolicies(new String[] {policy1.name()}, null);

    // Test list associated policies for schema
    String[] policies1 = schema.supportsPolicies().listPolicies();
    Assertions.assertEquals(Sets.newHashSet(policy1.name()), Sets.newHashSet(policies1));

    // Test associate policies to schema
    String[] policies =
        schema
            .supportsPolicies()
            .associatePolicies(new String[] {policy1.name(), policy2.name()}, null);

    Assertions.assertEquals(2, policies.length);
    HashSet<String> expected = Sets.newHashSet(policy1.name(), policy2.name());
    Assertions.assertEquals(expected, Sets.newHashSet(policies));

    // Test list associated policies with details for schema
    Policy[] policies2 = schema.supportsPolicies().listPolicyInfos();
    Assertions.assertEquals(2, policies2.length);

    Set<Policy> nonInheritedPolicies =
        Arrays.stream(policies2)
            .filter(policy -> !policy.inherited().get())
            .collect(Collectors.toSet());
    Set<Policy> inheritedPolicies =
        Arrays.stream(policies2)
            .filter(policy -> policy.inherited().get())
            .collect(Collectors.toSet());

    Assertions.assertEquals(2, nonInheritedPolicies.size());
    Assertions.assertEquals(0, inheritedPolicies.size());
    Assertions.assertTrue(nonInheritedPolicies.contains(policy1));
    Assertions.assertTrue(nonInheritedPolicies.contains(policy2));
    Assertions.assertFalse(inheritedPolicies.contains(policy2));

    // Test get associated policy for schema
    Policy policy = schema.supportsPolicies().getPolicy(policy1.name());
    Assertions.assertEquals(policy1, policy);
    Assertions.assertFalse(policy.inherited().get());

    // Test get objects associated with policy
    Assertions.assertEquals(2, policy.associatedObjects().count());
    Set<MetadataObject> resultObjects = Sets.newHashSet(policy.associatedObjects().objects());
    Set<MetadataObject> expectedObjects =
        Sets.newHashSet(
            MetadataObjectDTO.builder()
                .withName(relationalCatalog.name())
                .withType(MetadataObject.Type.CATALOG)
                .build(),
            MetadataObjectDTO.builder()
                .withParent(relationalCatalog.name())
                .withName(schema.name())
                .withType(MetadataObject.Type.SCHEMA)
                .build());
    Assertions.assertEquals(expectedObjects, resultObjects);
  }

  @Test
  public void testAssociatePoliciesToTable() {
    PolicyContent content =
        PolicyContents.custom(
            ImmutableMap.of("rule1", "value1"), ImmutableSet.of(MetadataObject.Type.TABLE), null);
    Policy policy1 =
        metalake.createPolicy(
            GravitinoITUtils.genRandomName("policy_it_table_policy1"),
            "custom",
            null,
            true,
            content);
    Policy policy2 =
        metalake.createPolicy(
            GravitinoITUtils.genRandomName("policy_it_table_policy2"),
            "custom",
            null,
            true,
            content);
    Policy policy3 =
        metalake.createPolicy(
            GravitinoITUtils.genRandomName("policy_it_table_policy3"),
            "custom",
            null,
            true,
            content);

    // Associate policies to catalog
    relationalCatalog.supportsPolicies().associatePolicies(new String[] {policy1.name()}, null);

    // Associate policies to schema
    schema.supportsPolicies().associatePolicies(new String[] {policy2.name()}, null);

    // Test associate policies to table
    String[] policies =
        table.supportsPolicies().associatePolicies(new String[] {policy3.name()}, null);

    Assertions.assertEquals(1, policies.length);
    Assertions.assertEquals(policy3.name(), policies[0]);

    // Test list associated policies for table
    String[] policies1 = table.supportsPolicies().listPolicies();
    Assertions.assertEquals(3, policies1.length);
    Set<String> policyNames = Sets.newHashSet(policies1);
    Assertions.assertTrue(policyNames.contains(policy1.name()));
    Assertions.assertTrue(policyNames.contains(policy2.name()));
    Assertions.assertTrue(policyNames.contains(policy3.name()));

    // Test list associated policies with details for table
    Policy[] policies2 = table.supportsPolicies().listPolicyInfos();
    Assertions.assertEquals(3, policies2.length);

    Set<Policy> nonInheritedPolicies =
        Arrays.stream(policies2)
            .filter(policy -> !policy.inherited().get())
            .collect(Collectors.toSet());
    Set<Policy> inheritedPolicies =
        Arrays.stream(policies2)
            .filter(policy -> policy.inherited().get())
            .collect(Collectors.toSet());

    Assertions.assertEquals(1, nonInheritedPolicies.size());
    Assertions.assertEquals(2, inheritedPolicies.size());
    Assertions.assertTrue(nonInheritedPolicies.contains(policy3));
    Assertions.assertTrue(inheritedPolicies.contains(policy1));
    Assertions.assertTrue(inheritedPolicies.contains(policy2));

    // Test get associated policy for table
    Policy resultPolicy1 = table.supportsPolicies().getPolicy(policy1.name());
    Assertions.assertEquals(policy1, resultPolicy1);
    Assertions.assertTrue(resultPolicy1.inherited().get());

    Policy resultPolicy2 = table.supportsPolicies().getPolicy(policy2.name());
    Assertions.assertEquals(policy2, resultPolicy2);
    Assertions.assertTrue(resultPolicy2.inherited().get());

    Policy resultPolicy3 = table.supportsPolicies().getPolicy(policy3.name());
    Assertions.assertEquals(policy3, resultPolicy3);
    Assertions.assertFalse(resultPolicy3.inherited().get());

    // Test get objects associated with policy
    Assertions.assertEquals(1, policy1.associatedObjects().count());
    Assertions.assertEquals(
        relationalCatalog.name(), policy1.associatedObjects().objects()[0].name());
    Assertions.assertEquals(
        MetadataObject.Type.CATALOG, policy1.associatedObjects().objects()[0].type());

    Assertions.assertEquals(1, policy2.associatedObjects().count());
    Assertions.assertEquals(schema.name(), policy2.associatedObjects().objects()[0].name());
    Assertions.assertEquals(
        MetadataObject.Type.SCHEMA, policy2.associatedObjects().objects()[0].type());

    Assertions.assertEquals(1, policy3.associatedObjects().count());
    Assertions.assertEquals(table.name(), policy3.associatedObjects().objects()[0].name());
    Assertions.assertEquals(
        MetadataObject.Type.TABLE, policy3.associatedObjects().objects()[0].type());
  }

  @Test
  public void testAssociateAndDeletePolicies() {
    Policy policy1 = createCustomPolicy(GravitinoITUtils.genRandomName("policy_it_policy1"));
    Policy policy2 = createCustomPolicy(GravitinoITUtils.genRandomName("policy_it_policy2"));
    Policy policy3 = createCustomPolicy(GravitinoITUtils.genRandomName("policy_it_policy3"));

    String[] associatedPolicies =
        relationalCatalog
            .supportsPolicies()
            .associatePolicies(
                new String[] {policy1.name(), policy2.name()}, new String[] {policy3.name()});

    Assertions.assertEquals(2, associatedPolicies.length);
    Set<String> policyNames = Sets.newHashSet(associatedPolicies);
    Assertions.assertTrue(policyNames.contains(policy1.name()));
    Assertions.assertTrue(policyNames.contains(policy2.name()));
    Assertions.assertFalse(policyNames.contains(policy3.name()));

    Policy retrievedPolicy = relationalCatalog.supportsPolicies().getPolicy(policy2.name());
    Assertions.assertEquals(policy2.name(), retrievedPolicy.name());
    Assertions.assertEquals(policy2.comment(), retrievedPolicy.comment());

    boolean deleted = metalake.deletePolicy("null");
    Assertions.assertFalse(deleted);

    deleted = metalake.deletePolicy(policy1.name());
    Assertions.assertTrue(deleted);
    deleted = metalake.deletePolicy(policy1.name());
    Assertions.assertFalse(deleted);

    String[] associatedPolicies1 = relationalCatalog.supportsPolicies().listPolicies();
    Assertions.assertArrayEquals(new String[] {policy2.name()}, associatedPolicies1);
  }

  @Test
  public void testAssociatePoliciesToModel() {
    Policy policy1 = createCustomPolicy(GravitinoITUtils.genRandomName("policy_it_model_policy1"));
    Policy policy2 = createCustomPolicy(GravitinoITUtils.genRandomName("policy_it_model_policy2"));
    Policy policy3 = createCustomPolicy(GravitinoITUtils.genRandomName("policy_it_model_policy3"));

    // Associate policies to catalog
    modelCatalog.supportsPolicies().associatePolicies(new String[] {policy1.name()}, null);

    // Associate policies to schema
    modelSchema.supportsPolicies().associatePolicies(new String[] {policy2.name()}, null);

    // Associate policies to model
    model.supportsPolicies().associatePolicies(new String[] {policy3.name()}, null);

    // Test list associated policies for model
    String[] policies1 = model.supportsPolicies().listPolicies();
    Assertions.assertEquals(3, policies1.length);
    Set<String> policyNames = Sets.newHashSet(policies1);
    Assertions.assertTrue(policyNames.contains(policy1.name()));
    Assertions.assertTrue(policyNames.contains(policy2.name()));
    Assertions.assertTrue(policyNames.contains(policy3.name()));

    // Test list associated policies with details for model
    Policy[] policies2 = model.supportsPolicies().listPolicyInfos();
    Assertions.assertEquals(3, policies2.length);

    Set<Policy> nonInheritedPolicies =
        Arrays.stream(policies2)
            .filter(policy -> !policy.inherited().get())
            .collect(Collectors.toSet());
    Set<Policy> inheritedPolicies =
        Arrays.stream(policies2)
            .filter(policy -> policy.inherited().get())
            .collect(Collectors.toSet());

    Assertions.assertEquals(1, nonInheritedPolicies.size());
    Assertions.assertEquals(2, inheritedPolicies.size());
    Assertions.assertTrue(nonInheritedPolicies.contains(policy3));
    Assertions.assertTrue(inheritedPolicies.contains(policy1));
    Assertions.assertTrue(inheritedPolicies.contains(policy2));

    // Test get associated policy for model
    Policy resultPolicy1 = model.supportsPolicies().getPolicy(policy1.name());
    Assertions.assertEquals(policy1, resultPolicy1);
    Assertions.assertTrue(resultPolicy1.inherited().get());

    Policy resultPolicy2 = model.supportsPolicies().getPolicy(policy2.name());
    Assertions.assertEquals(policy2, resultPolicy2);
    Assertions.assertTrue(resultPolicy2.inherited().get());

    Policy resultPolicy3 = model.supportsPolicies().getPolicy(policy3.name());
    Assertions.assertEquals(policy3, resultPolicy3);
    Assertions.assertFalse(resultPolicy3.inherited().get());

    // Test get objects associated with policy
    Assertions.assertEquals(1, policy1.associatedObjects().count());
    Assertions.assertEquals(modelCatalog.name(), policy1.associatedObjects().objects()[0].name());
    Assertions.assertEquals(
        MetadataObject.Type.CATALOG, policy1.associatedObjects().objects()[0].type());

    Assertions.assertEquals(1, policy2.associatedObjects().count());
    Assertions.assertEquals(modelSchema.name(), policy2.associatedObjects().objects()[0].name());
    Assertions.assertEquals(
        MetadataObject.Type.SCHEMA, policy2.associatedObjects().objects()[0].type());

    Assertions.assertEquals(1, policy3.associatedObjects().count());
    Assertions.assertEquals(model.name(), policy3.associatedObjects().objects()[0].name());
    Assertions.assertEquals(
        MetadataObject.Type.MODEL, policy3.associatedObjects().objects()[0].type());
  }

  private Policy createCustomPolicy(String name) {
    return metalake.createPolicy(
        name,
        "custom",
        "test comment",
        true,
        PolicyContents.custom(
            ImmutableMap.of("rule1", "value1"),
            ImmutableSet.of(MetadataObject.Type.CATALOG),
            null));
  }
}
