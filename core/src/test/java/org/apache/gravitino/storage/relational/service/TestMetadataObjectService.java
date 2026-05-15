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
package org.apache.gravitino.storage.relational.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.gravitino.Entity;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.function.FunctionDefinition;
import org.apache.gravitino.function.FunctionDefinitions;
import org.apache.gravitino.function.FunctionImpl;
import org.apache.gravitino.function.FunctionImpls;
import org.apache.gravitino.function.FunctionParam;
import org.apache.gravitino.function.FunctionParams;
import org.apache.gravitino.function.FunctionType;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.FunctionEntity;
import org.apache.gravitino.meta.GenericEntity;
import org.apache.gravitino.meta.JobTemplateEntity;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.policy.PolicyContent;
import org.apache.gravitino.policy.PolicyContents;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

public class TestMetadataObjectService extends TestJDBCBackend {
  private static final String METALAKE_NAME = "metalake_for_metadata_object_test";

  private final Set<MetadataObject.Type> supportedObjectTypes =
      ImmutableSet.of(
          MetadataObject.Type.CATALOG,
          MetadataObject.Type.SCHEMA,
          MetadataObject.Type.TABLE,
          MetadataObject.Type.FILESET,
          MetadataObject.Type.MODEL,
          MetadataObject.Type.TOPIC);
  private final PolicyContent policyContent =
      PolicyContents.custom(ImmutableMap.of("field", 123), supportedObjectTypes, null);

  @BeforeEach
  public void prepare() throws IOException {
    createAndInsertMakeLake(METALAKE_NAME);
  }

  @TestTemplate
  public void testBatchGetPolicyObjectsFullName() throws IOException {

    // Create multiple policies
    String policyOne = "policy_one";
    String policyTwo = "policy_two";
    PolicyEntity policy1 =
        createAndInsertPolicyEntity(policyOne, "test policy 1", policyContent, METALAKE_NAME);
    createAndInsertPolicyEntity(policyTwo, "test policy 2", policyContent, METALAKE_NAME);

    // Batch query using MetadataObjectService to fetch only one Entity belongs to policy1
    List<GenericEntity> genericEntities =
        List.of(
            GenericEntity.builder()
                .withId(policy1.id())
                .withName(policy1.name())
                .withNamespace(policy1.namespace())
                .withEntityType(Entity.EntityType.POLICY)
                .build());

    // Call MetadataObjectService.fromGenericEntities which will internally call
    // listPolicyPOsByPolicyIds
    List<MetadataObject> metadataObjects =
        MetadataObjectService.fromGenericEntities(genericEntities);

    // Verify results
    assertEquals(1, metadataObjects.size());
    Set<String> policyNames =
        metadataObjects.stream().map(MetadataObject::name).collect(Collectors.toSet());
    assertTrue(policyNames.contains(policyOne));

    // Verify all are POLICY type
    assertTrue(metadataObjects.stream().allMatch(obj -> obj.type() == MetadataObject.Type.POLICY));
  }

  @TestTemplate
  public void testBatchGetMixedObjectsFullName() throws IOException {
    String policyOne = "policy_one";
    String policyTwo = "policy_two";
    String catalogOne = "catalog_one";
    PolicyEntity policy1 =
        createAndInsertPolicyEntity(policyOne, "test policy 1", policyContent, METALAKE_NAME);
    createAndInsertPolicyEntity(policyTwo, "test policy 2", policyContent, METALAKE_NAME);
    CatalogEntity catalog = createAndInsertCatalog(METALAKE_NAME, catalogOne);

    List<GenericEntity> mixedEntities =
        List.of(
            GenericEntity.builder()
                .withId(policy1.id())
                .withName(policy1.name())
                .withNamespace(policy1.namespace())
                .withEntityType(Entity.EntityType.POLICY)
                .build(),
            GenericEntity.builder()
                .withId(catalog.id())
                .withName(catalog.name())
                .withNamespace(catalog.namespace())
                .withEntityType(Entity.EntityType.CATALOG)
                .build());

    List<MetadataObject> mixedResult = MetadataObjectService.fromGenericEntities(mixedEntities);
    assertEquals(2, mixedResult.size());

    Map<MetadataObject.Type, Long> typeCounts =
        mixedResult.stream()
            .collect(Collectors.groupingBy(MetadataObject::type, Collectors.counting()));
    assertEquals(1L, typeCounts.get(MetadataObject.Type.POLICY));
    assertEquals(1L, typeCounts.get(MetadataObject.Type.CATALOG));

    // Verify names match
    MetadataObject policyObject =
        mixedResult.stream()
            .filter(obj -> obj.type() == MetadataObject.Type.POLICY)
            .findFirst()
            .orElseThrow();
    assertEquals(policyOne, policyObject.name());

    MetadataObject catalogObject =
        mixedResult.stream()
            .filter(obj -> obj.type() == MetadataObject.Type.CATALOG)
            .findFirst()
            .orElseThrow();
    assertEquals(catalogOne, catalogObject.name());
  }

  @TestTemplate
  public void testBatchGetMetadataObjectsFullNameWithEmptyList() {
    // Test with empty list - should return empty result
    List<MetadataObject> emptyResult = MetadataObjectService.fromGenericEntities(List.of());
    assertEquals(0, emptyResult.size());
  }

  @TestTemplate
  public void testBatchGetTagObjectsFullName() throws IOException {

    // Create multiple tags
    String tagOne = "tag_one";
    String tagTwo = "tag_two";
    TagEntity tag1 = createAndInsertTagEntity(tagOne, "test tag 1", METALAKE_NAME);
    TagEntity tag2 = createAndInsertTagEntity(tagTwo, "test tag 2", METALAKE_NAME);
    // Test batch query using MetadataObjectService
    List<GenericEntity> genericEntities =
        List.of(
            GenericEntity.builder()
                .withId(tag1.id())
                .withName(tag1.name())
                .withNamespace(tag1.namespace())
                .withEntityType(Entity.EntityType.TAG)
                .build(),
            GenericEntity.builder()
                .withId(tag2.id())
                .withName(tag2.name())
                .withNamespace(tag2.namespace())
                .withEntityType(Entity.EntityType.TAG)
                .build());

    List<MetadataObject> metadataObjects =
        MetadataObjectService.fromGenericEntities(genericEntities);

    // Verify results
    assertEquals(2, metadataObjects.size());
    Set<String> tagNames =
        metadataObjects.stream().map(MetadataObject::name).collect(Collectors.toSet());
    Set<String> expected = Set.of(tagOne, tagTwo);
    assertEquals(expected, tagNames);

    // Verify all are TAG type
    assertTrue(metadataObjects.stream().allMatch(obj -> obj.type() == MetadataObject.Type.TAG));
  }

  @TestTemplate
  public void testBatchGetJobTemplateObjectsFullName() throws IOException {
    // Create multiple job templates
    String shellTemplate = "shell_template";
    String sparkTemplate = "spark_template";
    JobTemplateEntity shellJObTemplate =
        createAndInsertShellJobTemplateEntity(shellTemplate, "A shell job template", METALAKE_NAME);
    JobTemplateEntity sparkJobTemplate =
        createAndInsertSparkJobTemplateEntity(sparkTemplate, "A spark job template", METALAKE_NAME);

    // Test batch query using MetadataObjectService
    List<GenericEntity> jobTemplateEntities =
        List.of(
            GenericEntity.builder()
                .withId(shellJObTemplate.id())
                .withName(shellJObTemplate.name())
                .withNamespace(shellJObTemplate.namespace())
                .withEntityType(Entity.EntityType.JOB_TEMPLATE)
                .build(),
            GenericEntity.builder()
                .withId(sparkJobTemplate.id())
                .withName(sparkJobTemplate.name())
                .withNamespace(sparkJobTemplate.namespace())
                .withEntityType(Entity.EntityType.JOB_TEMPLATE)
                .build());

    List<MetadataObject> metadataObjects =
        MetadataObjectService.fromGenericEntities(jobTemplateEntities);

    // Verify results
    assertEquals(2, metadataObjects.size());
    Set<String> jobTemplateNames =
        metadataObjects.stream().map(MetadataObject::name).collect(Collectors.toSet());

    Set<String> expected = Set.of(shellTemplate, sparkTemplate);
    assertEquals(expected, jobTemplateNames);

    // Verify all are JOB_TEMPLATE type
    assertTrue(
        metadataObjects.stream().allMatch(obj -> obj.type() == MetadataObject.Type.JOB_TEMPLATE));
  }

  @TestTemplate
  public void testBatchGetTableObjectsFullName() throws IOException {
    // Create catalog and schema
    String catalogName = "test_catalog";
    String schemaName = "test_schema";
    String testTableOne = "test_table_one";
    String testTableTwo = "test_table_two";
    Namespace namespace = Namespace.of(METALAKE_NAME, catalogName, schemaName);
    createAndInsertCatalog(METALAKE_NAME, catalogName);
    createAndInsertSchema(METALAKE_NAME, catalogName, schemaName);
    // Create multiple tables
    TableEntity table1 = createAndInsertTableEntity(namespace, testTableOne);
    TableEntity table2 = createAndInsertTableEntity(namespace, testTableTwo);

    // Test batch query using MetadataObjectService
    List<GenericEntity> tableEntities =
        List.of(
            GenericEntity.builder()
                .withId(table1.id())
                .withName(table1.name())
                .withNamespace(table1.namespace())
                .withEntityType(Entity.EntityType.TABLE)
                .build(),
            GenericEntity.builder()
                .withId(table2.id())
                .withName(table2.name())
                .withNamespace(table2.namespace())
                .withEntityType(Entity.EntityType.TABLE)
                .build());

    List<MetadataObject> metadataObjects = MetadataObjectService.fromGenericEntities(tableEntities);

    // Verify results
    assertEquals(2, metadataObjects.size());

    // Table full names include catalog.schema.table format
    Set<String> tableFullNames =
        metadataObjects.stream().map(MetadataObject::fullName).collect(Collectors.toSet());

    Set<String> expected =
        Set.of(
            catalogName + "." + schemaName + "." + testTableOne,
            catalogName + "." + schemaName + "." + testTableTwo);
    assertEquals(expected, tableFullNames);

    // Verify all are TABLE type
    assertTrue(metadataObjects.stream().allMatch(obj -> obj.type() == MetadataObject.Type.TABLE));
  }

  @TestTemplate
  public void testBatchGetFunctionObjectsFullName() throws IOException {
    String catalogName = "test_function_catalog";
    String schemaName = "test_function_schema";
    String functionOne = "function_one";
    String functionTwo = "function_two";
    Namespace namespace = Namespace.of(METALAKE_NAME, catalogName, schemaName);
    createAndInsertCatalog(METALAKE_NAME, catalogName);
    createAndInsertSchema(METALAKE_NAME, catalogName, schemaName);

    FunctionEntity functionEntityOne = createAndInsertFunctionEntity(namespace, functionOne);
    FunctionEntity functionEntityTwo = createAndInsertFunctionEntity(namespace, functionTwo);

    List<GenericEntity> functionEntities =
        List.of(
            GenericEntity.builder()
                .withId(functionEntityOne.id())
                .withName(functionEntityOne.name())
                .withNamespace(functionEntityOne.namespace())
                .withEntityType(Entity.EntityType.FUNCTION)
                .build(),
            GenericEntity.builder()
                .withId(functionEntityTwo.id())
                .withName(functionEntityTwo.name())
                .withNamespace(functionEntityTwo.namespace())
                .withEntityType(Entity.EntityType.FUNCTION)
                .build());

    List<MetadataObject> metadataObjects =
        MetadataObjectService.fromGenericEntities(functionEntities);

    assertEquals(2, metadataObjects.size());
    assertTrue(
        metadataObjects.stream().allMatch(obj -> obj.type() == MetadataObject.Type.FUNCTION));

    Set<String> functionFullNames =
        metadataObjects.stream().map(MetadataObject::fullName).collect(Collectors.toSet());
    assertEquals(
        Set.of(
            catalogName + "." + schemaName + "." + functionOne,
            catalogName + "." + schemaName + "." + functionTwo),
        functionFullNames);
  }

  @TestTemplate
  public void testBatchGetFunctionObjectsFullNameWithMissingSchema() throws IOException {
    String catalogName = "test_function_catalog_missing_parent";
    String schemaName = "test_function_schema_missing_parent";
    String functionName = "function_with_missing_schema";
    Namespace namespace = Namespace.of(METALAKE_NAME, catalogName, schemaName);
    createAndInsertCatalog(METALAKE_NAME, catalogName);
    createAndInsertSchema(METALAKE_NAME, catalogName, schemaName);

    FunctionEntity functionEntity = createAndInsertFunctionEntity(namespace, functionName);
    Long schemaId =
        EntityIdService.getEntityId(
            NameIdentifier.of(METALAKE_NAME, catalogName, schemaName), Entity.EntityType.SCHEMA);
    softDeleteSchemaMeta(schemaId);

    List<GenericEntity> functionEntities =
        List.of(
            GenericEntity.builder()
                .withId(functionEntity.id())
                .withName(functionEntity.name())
                .withNamespace(functionEntity.namespace())
                .withEntityType(Entity.EntityType.FUNCTION)
                .build());

    List<MetadataObject> metadataObjects =
        MetadataObjectService.fromGenericEntities(functionEntities);

    assertTrue(metadataObjects.isEmpty());
  }

  private FunctionEntity createAndInsertFunctionEntity(Namespace namespace, String functionName)
      throws IOException {
    FunctionEntity functionEntity =
        FunctionEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName(functionName)
            .withNamespace(namespace)
            .withComment("function comment")
            .withFunctionType(FunctionType.SCALAR)
            .withDeterministic(true)
            .withDefinitions(new FunctionDefinition[] {createFunctionDefinition()})
            .withAuditInfo(AUDIT_INFO)
            .build();
    backend.insert(functionEntity, false);
    return functionEntity;
  }

  private FunctionDefinition createFunctionDefinition() {
    FunctionParam inputParam = FunctionParams.of("param1", Types.IntegerType.get());
    FunctionImpl functionImpl =
        FunctionImpls.ofSql(FunctionImpl.RuntimeType.SPARK, "SELECT param1 + 1");
    return FunctionDefinitions.of(
        new FunctionParam[] {inputParam},
        Types.IntegerType.get(),
        new FunctionImpl[] {functionImpl});
  }

  private void softDeleteSchemaMeta(Long schemaId) {
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          String.format(
              "UPDATE schema_meta SET deleted_at = %d WHERE schema_id = %d",
              Instant.now().toEpochMilli(), schemaId));
    } catch (SQLException e) {
      throw new RuntimeException("Failed to soft delete schema metadata for test setup", e);
    }
  }
}
