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
import java.util.Collections;
import java.util.Set;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Schema;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.exceptions.NoSuchPolicyException;
import org.apache.gravitino.exceptions.PolicyAlreadyExistsException;
import org.apache.gravitino.function.Function;
import org.apache.gravitino.function.FunctionDefinition;
import org.apache.gravitino.function.FunctionDefinitions;
import org.apache.gravitino.function.FunctionImpl;
import org.apache.gravitino.function.FunctionImpls;
import org.apache.gravitino.function.FunctionParam;
import org.apache.gravitino.function.FunctionParams;
import org.apache.gravitino.function.FunctionType;
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
import org.apache.gravitino.rel.Dialects;
import org.apache.gravitino.rel.SQLRepresentation;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.View;
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
  private static View view;
  private static Function function;

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

    // Create view
    String viewName = GravitinoITUtils.genRandomName("policy_it_view");
    Assertions.assertFalse(
        relationalCatalog.asViewCatalog().viewExists(NameIdentifier.of(schemaName, viewName)));
    view =
        relationalCatalog
            .asViewCatalog()
            .createView(
                NameIdentifier.of(schemaName, viewName),
                "comment",
                new Column[] {
                  Column.of("col1", Types.IntegerType.get()),
                  Column.of("col2", Types.StringType.get())
                },
                new SQLRepresentation[] {
                  SQLRepresentation.builder()
                      .withDialect(Dialects.HIVE)
                      .withSql("SELECT col1, col2 FROM " + table.name())
                      .build()
                },
                null,
                null,
                Collections.emptyMap());

    // Create function
    String functionName = GravitinoITUtils.genRandomName("policy_it_function");
    Assertions.assertFalse(
        relationalCatalog
            .asFunctionCatalog()
            .functionExists(NameIdentifier.of(schemaName, functionName)));
    FunctionParam param = FunctionParams.of("x", Types.IntegerType.get());
    FunctionImpl impl = FunctionImpls.ofSql(FunctionImpl.RuntimeType.SPARK, "SELECT x + 1");
    FunctionDefinition definition =
        FunctionDefinitions.of(
            new FunctionParam[] {param}, Types.IntegerType.get(), new FunctionImpl[] {impl});
    function =
        relationalCatalog
            .asFunctionCatalog()
            .registerFunction(
                NameIdentifier.of(schemaName, functionName),
                "comment",
                FunctionType.SCALAR,
                true,
                new FunctionDefinition[] {definition});

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
    relationalCatalog
        .asFunctionCatalog()
        .dropFunction(NameIdentifier.of(schema.name(), function.name()));
    relationalCatalog.asViewCatalog().dropView(NameIdentifier.of(schema.name(), view.name()));
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
    Assertions.assertEquals(content.rules(), policy.content().rules());

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
    Assertions.assertEquals(content.rules(), fetchedPolicy.content().rules());

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
}
