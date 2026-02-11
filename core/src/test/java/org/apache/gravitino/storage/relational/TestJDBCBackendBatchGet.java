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
package org.apache.gravitino.storage.relational;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.job.JobHandle;
import org.apache.gravitino.job.JobTemplate;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.FilesetEntity;
import org.apache.gravitino.meta.JobEntity;
import org.apache.gravitino.meta.JobTemplateEntity;
import org.apache.gravitino.meta.ModelEntity;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.meta.TopicEntity;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.utils.NamespaceUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;

/** Tests for batch-get operations in JDBCBackend. */
public class TestJDBCBackendBatchGet extends TestJDBCBackend {

  @TestTemplate
  public void testBatchGetMetalakes() throws IOException {
    // Create metalakes
    BaseMetalake metalake1 =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), "metalake1", AUDIT_INFO);
    BaseMetalake metalake2 =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), "metalake2", AUDIT_INFO);
    BaseMetalake metalake3 =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), "metalake3", AUDIT_INFO);

    backend.insert(metalake1, false);
    backend.insert(metalake2, false);
    backend.insert(metalake3, false);

    // Test batch get
    List<NameIdentifier> identifiers =
        Lists.newArrayList(
            metalake1.nameIdentifier(), metalake2.nameIdentifier(), metalake3.nameIdentifier());

    List<BaseMetalake> result = backend.batchGet(identifiers, Entity.EntityType.METALAKE);

    Assertions.assertEquals(3, result.size());
    Map<String, BaseMetalake> resultMap =
        result.stream().collect(Collectors.toMap(BaseMetalake::name, m -> m));

    // Verify metalake1
    BaseMetalake retrieved1 = resultMap.get("metalake1");
    Assertions.assertNotNull(retrieved1);
    Assertions.assertEquals(metalake1.id(), retrieved1.id());
    Assertions.assertEquals(metalake1.name(), retrieved1.name());
    Assertions.assertEquals(metalake1.auditInfo().creator(), retrieved1.auditInfo().creator());

    // Verify metalake2
    BaseMetalake retrieved2 = resultMap.get("metalake2");
    Assertions.assertNotNull(retrieved2);
    Assertions.assertEquals(metalake2.id(), retrieved2.id());
    Assertions.assertEquals(metalake2.name(), retrieved2.name());
    Assertions.assertEquals(metalake2.auditInfo().creator(), retrieved2.auditInfo().creator());

    // Verify metalake3
    BaseMetalake retrieved3 = resultMap.get("metalake3");
    Assertions.assertNotNull(retrieved3);
    Assertions.assertEquals(metalake3.id(), retrieved3.id());
    Assertions.assertEquals(metalake3.name(), retrieved3.name());
    Assertions.assertEquals(metalake3.auditInfo().creator(), retrieved3.auditInfo().creator());
  }

  @TestTemplate
  public void testBatchGetCatalogs() throws IOException {
    // Setup
    String metalakeName = "metalake_for_catalog_batch";
    createAndInsertMakeLake(metalakeName);

    // Create catalogs
    CatalogEntity catalog1 =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofCatalog(metalakeName),
            "catalog1",
            AUDIT_INFO);
    CatalogEntity catalog2 =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofCatalog(metalakeName),
            "catalog2",
            AUDIT_INFO);
    CatalogEntity catalog3 =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofCatalog(metalakeName),
            "catalog3",
            AUDIT_INFO);

    backend.insert(catalog1, false);
    backend.insert(catalog2, false);
    backend.insert(catalog3, false);

    // Test batch get
    List<NameIdentifier> identifiers =
        Lists.newArrayList(
            catalog1.nameIdentifier(), catalog2.nameIdentifier(), catalog3.nameIdentifier());

    List<CatalogEntity> result = backend.batchGet(identifiers, Entity.EntityType.CATALOG);

    Assertions.assertEquals(3, result.size());
    Map<String, CatalogEntity> resultMap =
        result.stream().collect(Collectors.toMap(CatalogEntity::name, c -> c));

    // Verify catalog1
    CatalogEntity retrieved1 = resultMap.get("catalog1");
    Assertions.assertNotNull(retrieved1);
    Assertions.assertEquals(catalog1.id(), retrieved1.id());
    Assertions.assertEquals(catalog1.name(), retrieved1.name());
    Assertions.assertEquals(catalog1.namespace(), retrieved1.namespace());
    Assertions.assertEquals(catalog1.getType(), retrieved1.getType());
    Assertions.assertEquals(catalog1.getProvider(), retrieved1.getProvider());

    // Verify catalog2
    CatalogEntity retrieved2 = resultMap.get("catalog2");
    Assertions.assertNotNull(retrieved2);
    Assertions.assertEquals(catalog2.id(), retrieved2.id());
    Assertions.assertEquals(catalog2.name(), retrieved2.name());
    Assertions.assertEquals(catalog2.namespace(), retrieved2.namespace());

    // Verify catalog3
    CatalogEntity retrieved3 = resultMap.get("catalog3");
    Assertions.assertNotNull(retrieved3);
    Assertions.assertEquals(catalog3.id(), retrieved3.id());
    Assertions.assertEquals(catalog3.name(), retrieved3.name());
    Assertions.assertEquals(catalog3.namespace(), retrieved3.namespace());
  }

  @TestTemplate
  public void testBatchGetSchemas() throws IOException {
    // Setup
    String metalakeName = "metalake_for_schema_batch";
    String catalogName = "catalog_for_schema_batch";
    createAndInsertMakeLake(metalakeName);
    createAndInsertCatalog(metalakeName, catalogName);

    // Create schemas
    SchemaEntity schema1 =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofSchema(metalakeName, catalogName),
            "schema1",
            AUDIT_INFO);
    SchemaEntity schema2 =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofSchema(metalakeName, catalogName),
            "schema2",
            AUDIT_INFO);

    backend.insert(schema1, false);
    backend.insert(schema2, false);

    // Test batch get
    List<NameIdentifier> identifiers =
        Lists.newArrayList(schema1.nameIdentifier(), schema2.nameIdentifier());

    List<SchemaEntity> result = backend.batchGet(identifiers, Entity.EntityType.SCHEMA);

    Assertions.assertEquals(2, result.size());
    Map<String, SchemaEntity> resultMap =
        result.stream().collect(Collectors.toMap(SchemaEntity::name, s -> s));

    // Verify schema1
    SchemaEntity retrieved1 = resultMap.get("schema1");
    Assertions.assertNotNull(retrieved1);
    Assertions.assertEquals(schema1.id(), retrieved1.id());
    Assertions.assertEquals(schema1.name(), retrieved1.name());
    Assertions.assertEquals(schema1.namespace(), retrieved1.namespace());
    Assertions.assertEquals(schema1.auditInfo().creator(), retrieved1.auditInfo().creator());

    // Verify schema2
    SchemaEntity retrieved2 = resultMap.get("schema2");
    Assertions.assertNotNull(retrieved2);
    Assertions.assertEquals(schema2.id(), retrieved2.id());
    Assertions.assertEquals(schema2.name(), retrieved2.name());
    Assertions.assertEquals(schema2.namespace(), retrieved2.namespace());
  }

  @TestTemplate
  public void testBatchGetTables() throws IOException {
    // Setup
    String metalakeName = "metalake_for_table_batch";
    String catalogName = "catalog_for_table_batch";
    String schemaName = "schema_for_table_batch";
    createParentEntities(metalakeName, catalogName, schemaName, AUDIT_INFO);

    // Create tables
    TableEntity table1 =
        createTableEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofTable(metalakeName, catalogName, schemaName),
            "table1",
            AUDIT_INFO);
    TableEntity table2 =
        createTableEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofTable(metalakeName, catalogName, schemaName),
            "table2",
            AUDIT_INFO);
    TableEntity table3 =
        createTableEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofTable(metalakeName, catalogName, schemaName),
            "table3",
            AUDIT_INFO);

    backend.insert(table1, false);
    backend.insert(table2, false);
    backend.insert(table3, false);

    // Test batch get
    List<NameIdentifier> identifiers =
        Lists.newArrayList(
            table1.nameIdentifier(), table2.nameIdentifier(), table3.nameIdentifier());

    List<TableEntity> result = backend.batchGet(identifiers, Entity.EntityType.TABLE);

    Assertions.assertEquals(3, result.size());
    Map<String, TableEntity> resultMap =
        result.stream().collect(Collectors.toMap(TableEntity::name, t -> t));

    // Verify table1
    TableEntity retrieved1 = resultMap.get("table1");
    Assertions.assertNotNull(retrieved1);
    Assertions.assertEquals(table1.id(), retrieved1.id());
    Assertions.assertEquals(table1.name(), retrieved1.name());
    Assertions.assertEquals(table1.namespace(), retrieved1.namespace());
    Assertions.assertEquals(table1.auditInfo().creator(), retrieved1.auditInfo().creator());

    // Verify table2
    TableEntity retrieved2 = resultMap.get("table2");
    Assertions.assertNotNull(retrieved2);
    Assertions.assertEquals(table2.id(), retrieved2.id());
    Assertions.assertEquals(table2.name(), retrieved2.name());
    Assertions.assertEquals(table2.namespace(), retrieved2.namespace());

    // Verify table3
    TableEntity retrieved3 = resultMap.get("table3");
    Assertions.assertNotNull(retrieved3);
    Assertions.assertEquals(table3.id(), retrieved3.id());
    Assertions.assertEquals(table3.name(), retrieved3.name());
    Assertions.assertEquals(table3.namespace(), retrieved3.namespace());
  }

  @TestTemplate
  public void testBatchGetFilesets() throws IOException {
    // Setup
    String metalakeName = "metalake_for_fileset_batch";
    String catalogName = "catalog_for_fileset_batch";
    String schemaName = "schema_for_fileset_batch";
    createParentEntities(metalakeName, catalogName, schemaName, AUDIT_INFO);

    // Create filesets
    FilesetEntity fileset1 =
        createFilesetEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofFileset(metalakeName, catalogName, schemaName),
            "fileset1",
            AUDIT_INFO);
    FilesetEntity fileset2 =
        createFilesetEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofFileset(metalakeName, catalogName, schemaName),
            "fileset2",
            AUDIT_INFO);

    backend.insert(fileset1, false);
    backend.insert(fileset2, false);

    // Test batch get
    List<NameIdentifier> identifiers =
        Lists.newArrayList(fileset1.nameIdentifier(), fileset2.nameIdentifier());

    List<FilesetEntity> result = backend.batchGet(identifiers, Entity.EntityType.FILESET);

    Assertions.assertEquals(2, result.size());
    Map<String, FilesetEntity> resultMap =
        result.stream().collect(Collectors.toMap(FilesetEntity::name, f -> f));

    // Verify fileset1
    FilesetEntity retrieved1 = resultMap.get("fileset1");
    Assertions.assertNotNull(retrieved1);
    Assertions.assertEquals(fileset1.id(), retrieved1.id());
    Assertions.assertEquals(fileset1.name(), retrieved1.name());
    Assertions.assertEquals(fileset1.namespace(), retrieved1.namespace());
    Assertions.assertEquals(fileset1.filesetType(), retrieved1.filesetType());
    Assertions.assertEquals(fileset1.storageLocation(), retrieved1.storageLocation());

    // Verify fileset2
    FilesetEntity retrieved2 = resultMap.get("fileset2");
    Assertions.assertNotNull(retrieved2);
    Assertions.assertEquals(fileset2.id(), retrieved2.id());
    Assertions.assertEquals(fileset2.name(), retrieved2.name());
    Assertions.assertEquals(fileset2.namespace(), retrieved2.namespace());
    Assertions.assertEquals(fileset2.filesetType(), retrieved2.filesetType());
  }

  @TestTemplate
  public void testBatchGetTopics() throws IOException {
    // Setup
    String metalakeName = "metalake_for_topic_batch";
    String catalogName = "catalog_for_topic_batch";
    String schemaName = "schema_for_topic_batch";
    createParentEntities(metalakeName, catalogName, schemaName, AUDIT_INFO);

    // Create topics
    TopicEntity topic1 =
        createTopicEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofTopic(metalakeName, catalogName, schemaName),
            "topic1",
            AUDIT_INFO);
    TopicEntity topic2 =
        createTopicEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofTopic(metalakeName, catalogName, schemaName),
            "topic2",
            AUDIT_INFO);

    backend.insert(topic1, false);
    backend.insert(topic2, false);

    // Test batch get
    List<NameIdentifier> identifiers =
        Lists.newArrayList(topic1.nameIdentifier(), topic2.nameIdentifier());

    List<TopicEntity> result = backend.batchGet(identifiers, Entity.EntityType.TOPIC);

    Assertions.assertEquals(2, result.size());
    Map<String, TopicEntity> resultMap =
        result.stream().collect(Collectors.toMap(TopicEntity::name, t -> t));

    // Verify topic1
    TopicEntity retrieved1 = resultMap.get("topic1");
    Assertions.assertNotNull(retrieved1);
    Assertions.assertEquals(topic1.id(), retrieved1.id());
    Assertions.assertEquals(topic1.name(), retrieved1.name());
    Assertions.assertEquals(topic1.namespace(), retrieved1.namespace());
    Assertions.assertEquals(topic1.comment(), retrieved1.comment());
    Assertions.assertEquals(topic1.properties(), retrieved1.properties());

    // Verify topic2
    TopicEntity retrieved2 = resultMap.get("topic2");
    Assertions.assertNotNull(retrieved2);
    Assertions.assertEquals(topic2.id(), retrieved2.id());
    Assertions.assertEquals(topic2.name(), retrieved2.name());
    Assertions.assertEquals(topic2.namespace(), retrieved2.namespace());
    Assertions.assertEquals(topic2.comment(), retrieved2.comment());
  }

  @TestTemplate
  public void testBatchGetModels() throws IOException {
    // Setup
    String metalakeName = "metalake_for_model_batch";
    String catalogName = "catalog_for_model_batch";
    String schemaName = "schema_for_model_batch";
    createParentEntities(metalakeName, catalogName, schemaName, AUDIT_INFO);

    // Create models
    ModelEntity model1 =
        createModelEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofModel(metalakeName, catalogName, schemaName),
            "model1",
            "comment1",
            0,
            ImmutableMap.of("key1", "value1"),
            AUDIT_INFO);
    ModelEntity model2 =
        createModelEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofModel(metalakeName, catalogName, schemaName),
            "model2",
            "comment2",
            0,
            ImmutableMap.of("key2", "value2"),
            AUDIT_INFO);

    backend.insert(model1, false);
    backend.insert(model2, false);

    // Test batch get
    List<NameIdentifier> identifiers =
        Lists.newArrayList(model1.nameIdentifier(), model2.nameIdentifier());

    List<ModelEntity> result = backend.batchGet(identifiers, Entity.EntityType.MODEL);

    Assertions.assertEquals(2, result.size());
    Map<String, ModelEntity> resultMap =
        result.stream().collect(Collectors.toMap(ModelEntity::name, m -> m));

    // Verify model1
    ModelEntity retrieved1 = resultMap.get("model1");
    Assertions.assertNotNull(retrieved1);
    Assertions.assertEquals(model1.id(), retrieved1.id());
    Assertions.assertEquals(model1.name(), retrieved1.name());
    Assertions.assertEquals(model1.namespace(), retrieved1.namespace());
    Assertions.assertEquals(model1.comment(), retrieved1.comment());
    Assertions.assertEquals(model1.latestVersion(), retrieved1.latestVersion());
    Assertions.assertEquals(model1.properties(), retrieved1.properties());

    // Verify model2
    ModelEntity retrieved2 = resultMap.get("model2");
    Assertions.assertNotNull(retrieved2);
    Assertions.assertEquals(model2.id(), retrieved2.id());
    Assertions.assertEquals(model2.name(), retrieved2.name());
    Assertions.assertEquals(model2.namespace(), retrieved2.namespace());
    Assertions.assertEquals(model2.comment(), retrieved2.comment());
    Assertions.assertEquals(model2.latestVersion(), retrieved2.latestVersion());
  }

  @TestTemplate
  public void testBatchGetTags() throws IOException {
    // Setup
    String metalakeName = "metalake_for_tag_batch";
    createAndInsertMakeLake(metalakeName);

    // Create tags
    TagEntity tag1 =
        TagEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("tag1")
            .withNamespace(Namespace.of(metalakeName))
            .withComment("comment1")
            .withProperties(new HashMap<>())
            .withAuditInfo(AUDIT_INFO)
            .build();
    TagEntity tag2 =
        TagEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("tag2")
            .withNamespace(Namespace.of(metalakeName))
            .withComment("comment2")
            .withProperties(new HashMap<>())
            .withAuditInfo(AUDIT_INFO)
            .build();

    backend.insert(tag1, false);
    backend.insert(tag2, false);

    // Test batch get
    List<NameIdentifier> identifiers =
        Lists.newArrayList(tag1.nameIdentifier(), tag2.nameIdentifier());

    List<TagEntity> result = backend.batchGet(identifiers, Entity.EntityType.TAG);

    Assertions.assertEquals(2, result.size());
    Map<String, TagEntity> resultMap =
        result.stream().collect(Collectors.toMap(TagEntity::name, t -> t));

    // Verify tag1
    TagEntity retrieved1 = resultMap.get("tag1");
    Assertions.assertNotNull(retrieved1);
    Assertions.assertEquals(tag1.id(), retrieved1.id());
    Assertions.assertEquals(tag1.name(), retrieved1.name());
    Assertions.assertEquals(tag1.namespace(), retrieved1.namespace());
    Assertions.assertEquals(tag1.comment(), retrieved1.comment());
    Assertions.assertEquals(tag1.properties(), retrieved1.properties());

    // Verify tag2
    TagEntity retrieved2 = resultMap.get("tag2");
    Assertions.assertNotNull(retrieved2);
    Assertions.assertEquals(tag2.id(), retrieved2.id());
    Assertions.assertEquals(tag2.name(), retrieved2.name());
    Assertions.assertEquals(tag2.namespace(), retrieved2.namespace());
    Assertions.assertEquals(tag2.comment(), retrieved2.comment());
  }

  @TestTemplate
  public void testBatchGetPolicies() throws IOException {
    // Setup
    String metalakeName = "metalake_for_policy_batch";
    createAndInsertMakeLake(metalakeName);

    // Create policies
    PolicyEntity policy1 =
        createPolicy(
            RandomIdGenerator.INSTANCE.nextId(), Namespace.of(metalakeName), "policy1", AUDIT_INFO);
    PolicyEntity policy2 =
        createPolicy(
            RandomIdGenerator.INSTANCE.nextId(), Namespace.of(metalakeName), "policy2", AUDIT_INFO);

    backend.insert(policy1, false);
    backend.insert(policy2, false);

    // Test batch get
    List<NameIdentifier> identifiers =
        Lists.newArrayList(policy1.nameIdentifier(), policy2.nameIdentifier());

    List<PolicyEntity> result = backend.batchGet(identifiers, Entity.EntityType.POLICY);

    Assertions.assertEquals(2, result.size());
    Map<String, PolicyEntity> resultMap =
        result.stream().collect(Collectors.toMap(PolicyEntity::name, p -> p));

    // Verify policy1
    PolicyEntity retrieved1 = resultMap.get("policy1");
    Assertions.assertNotNull(retrieved1);
    Assertions.assertEquals(policy1.id(), retrieved1.id());
    Assertions.assertEquals(policy1.name(), retrieved1.name());
    Assertions.assertEquals(policy1.namespace(), retrieved1.namespace());
    Assertions.assertEquals(policy1.policyType(), retrieved1.policyType());
    Assertions.assertEquals(policy1.enabled(), retrieved1.enabled());

    // Verify policy2
    PolicyEntity retrieved2 = resultMap.get("policy2");
    Assertions.assertNotNull(retrieved2);
    Assertions.assertEquals(policy2.id(), retrieved2.id());
    Assertions.assertEquals(policy2.name(), retrieved2.name());
    Assertions.assertEquals(policy2.namespace(), retrieved2.namespace());
    Assertions.assertEquals(policy2.policyType(), retrieved2.policyType());
  }

  @TestTemplate
  public void testBatchGetJobs() throws IOException {
    // Setup
    String metalakeName = "metalake_for_job_batch";
    createAndInsertMakeLake(metalakeName);

    // Create job templates first (jobs require existing templates)
    JobTemplateEntity.TemplateContent content1 =
        JobTemplateEntity.TemplateContent.builder()
            .withJobType(JobTemplate.JobType.SHELL)
            .withExecutable("/bin/bash")
            .withArguments(Lists.newArrayList("arg1"))
            .withEnvironments(ImmutableMap.of("ENV1", "value1"))
            .withCustomFields(new HashMap<>())
            .withScripts(Lists.newArrayList("echo 'test1'"))
            .build();

    JobTemplateEntity.TemplateContent content2 =
        JobTemplateEntity.TemplateContent.builder()
            .withJobType(JobTemplate.JobType.SHELL)
            .withExecutable("/bin/bash")
            .withArguments(Lists.newArrayList("arg2"))
            .withEnvironments(ImmutableMap.of("ENV2", "value2"))
            .withCustomFields(new HashMap<>())
            .withScripts(Lists.newArrayList("echo 'test2'"))
            .build();

    JobTemplateEntity template1 =
        JobTemplateEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("template1")
            .withNamespace(NamespaceUtil.ofJobTemplate(metalakeName))
            .withComment("comment1")
            .withTemplateContent(content1)
            .withAuditInfo(AUDIT_INFO)
            .build();

    JobTemplateEntity template2 =
        JobTemplateEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("template2")
            .withNamespace(NamespaceUtil.ofJobTemplate(metalakeName))
            .withComment("comment2")
            .withTemplateContent(content2)
            .withAuditInfo(AUDIT_INFO)
            .build();

    backend.insert(template1, false);
    backend.insert(template2, false);

    // Now create jobs that reference the templates
    JobEntity job1 =
        JobEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withJobExecutionId("exec-id-1")
            .withNamespace(NamespaceUtil.ofJob(metalakeName))
            .withStatus(JobHandle.Status.STARTED)
            .withJobTemplateName("template1")
            .withAuditInfo(AUDIT_INFO)
            .build();
    JobEntity job2 =
        JobEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withJobExecutionId("exec-id-2")
            .withNamespace(NamespaceUtil.ofJob(metalakeName))
            .withStatus(JobHandle.Status.QUEUED)
            .withJobTemplateName("template2")
            .withAuditInfo(AUDIT_INFO)
            .build();

    backend.insert(job1, false);
    backend.insert(job2, false);

    // Test batch get
    List<NameIdentifier> identifiers =
        Lists.newArrayList(job1.nameIdentifier(), job2.nameIdentifier());

    List<JobEntity> result = backend.batchGet(identifiers, Entity.EntityType.JOB);

    Assertions.assertEquals(2, result.size());
    Map<String, JobEntity> resultMap =
        result.stream().collect(Collectors.toMap(JobEntity::name, j -> j));

    // Verify job1
    JobEntity retrieved1 = resultMap.get(job1.name());
    Assertions.assertNotNull(retrieved1);
    Assertions.assertEquals(job1.id(), retrieved1.id());
    Assertions.assertEquals(job1.name(), retrieved1.name());
    Assertions.assertEquals(job1.namespace(), retrieved1.namespace());
    Assertions.assertEquals(job1.jobExecutionId(), retrieved1.jobExecutionId());
    Assertions.assertEquals(job1.status(), retrieved1.status());
    Assertions.assertEquals(job1.jobTemplateName(), retrieved1.jobTemplateName());

    // Verify job2
    JobEntity retrieved2 = resultMap.get(job2.name());
    Assertions.assertNotNull(retrieved2);
    Assertions.assertEquals(job2.id(), retrieved2.id());
    Assertions.assertEquals(job2.name(), retrieved2.name());
    Assertions.assertEquals(job2.namespace(), retrieved2.namespace());
    Assertions.assertEquals(job2.jobExecutionId(), retrieved2.jobExecutionId());
    Assertions.assertEquals(job2.status(), retrieved2.status());
  }

  @TestTemplate
  public void testBatchGetJobTemplates() throws IOException {
    // Setup
    String metalakeName = "metalake_for_jobtemplate_batch";
    createAndInsertMakeLake(metalakeName);

    // Create job templates with simple template content
    JobTemplateEntity.TemplateContent content1 =
        JobTemplateEntity.TemplateContent.builder()
            .withJobType(JobTemplate.JobType.SHELL)
            .withExecutable("/bin/bash")
            .withArguments(Lists.newArrayList("arg1"))
            .withEnvironments(ImmutableMap.of("ENV1", "value1"))
            .withCustomFields(new HashMap<>())
            .withScripts(Lists.newArrayList("echo 'test1'"))
            .build();

    JobTemplateEntity.TemplateContent content2 =
        JobTemplateEntity.TemplateContent.builder()
            .withJobType(JobTemplate.JobType.SHELL)
            .withExecutable("/bin/bash")
            .withArguments(Lists.newArrayList("arg2"))
            .withEnvironments(ImmutableMap.of("ENV2", "value2"))
            .withCustomFields(new HashMap<>())
            .withScripts(Lists.newArrayList("echo 'test2'"))
            .build();

    JobTemplateEntity template1 =
        JobTemplateEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("template1")
            .withNamespace(NamespaceUtil.ofJobTemplate(metalakeName))
            .withComment("comment1")
            .withTemplateContent(content1)
            .withAuditInfo(AUDIT_INFO)
            .build();

    JobTemplateEntity template2 =
        JobTemplateEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("template2")
            .withNamespace(NamespaceUtil.ofJobTemplate(metalakeName))
            .withComment("comment2")
            .withTemplateContent(content2)
            .withAuditInfo(AUDIT_INFO)
            .build();

    backend.insert(template1, false);
    backend.insert(template2, false);

    // Test batch get
    List<NameIdentifier> identifiers =
        Lists.newArrayList(template1.nameIdentifier(), template2.nameIdentifier());

    List<JobTemplateEntity> result = backend.batchGet(identifiers, Entity.EntityType.JOB_TEMPLATE);

    Assertions.assertEquals(2, result.size());
    Map<String, JobTemplateEntity> resultMap =
        result.stream().collect(Collectors.toMap(JobTemplateEntity::name, t -> t));

    // Verify template1
    JobTemplateEntity retrieved1 = resultMap.get("template1");
    Assertions.assertNotNull(retrieved1);
    Assertions.assertEquals(template1.id(), retrieved1.id());
    Assertions.assertEquals(template1.name(), retrieved1.name());
    Assertions.assertEquals(template1.namespace(), retrieved1.namespace());
    Assertions.assertEquals(template1.comment(), retrieved1.comment());
    Assertions.assertNotNull(retrieved1.templateContent());
    Assertions.assertEquals(JobTemplate.JobType.SHELL, retrieved1.templateContent().jobType());

    // Verify template2
    JobTemplateEntity retrieved2 = resultMap.get("template2");
    Assertions.assertNotNull(retrieved2);
    Assertions.assertEquals(template2.id(), retrieved2.id());
    Assertions.assertEquals(template2.name(), retrieved2.name());
    Assertions.assertEquals(template2.namespace(), retrieved2.namespace());
    Assertions.assertEquals(template2.comment(), retrieved2.comment());
    Assertions.assertNotNull(retrieved2.templateContent());
  }

  @TestTemplate
  public void testBatchGetEmptyList() {
    // Test with empty list
    List<NameIdentifier> emptyList = Lists.newArrayList();
    List<TableEntity> result = backend.batchGet(emptyList, Entity.EntityType.TABLE);
    Assertions.assertNotNull(result);
    Assertions.assertTrue(result.isEmpty());
  }

  @TestTemplate
  public void testBatchGetNullList() {
    // Test with null list
    List<TableEntity> result = backend.batchGet(null, Entity.EntityType.TABLE);
    Assertions.assertNotNull(result);
    Assertions.assertTrue(result.isEmpty());
  }

  @TestTemplate
  public void testBatchGetDifferentNamespaces() throws IOException {
    // Setup two different metalakes
    String metalakeName1 = "metalake_diff1";
    String metalakeName2 = "metalake_diff2";
    createAndInsertMakeLake(metalakeName1);
    createAndInsertMakeLake(metalakeName2);

    // Create catalogs in different namespaces
    CatalogEntity catalog1 =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofCatalog(metalakeName1),
            "catalog1",
            AUDIT_INFO);
    CatalogEntity catalog2 =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofCatalog(metalakeName2),
            "catalog2",
            AUDIT_INFO);

    backend.insert(catalog1, false);
    backend.insert(catalog2, false);

    // Test batch get with different namespaces - should throw exception
    List<NameIdentifier> identifiers =
        Lists.newArrayList(catalog1.nameIdentifier(), catalog2.nameIdentifier());

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> backend.batchGet(identifiers, Entity.EntityType.CATALOG),
        "All identifiers must have the same namespace for batch get operation");
  }

  @TestTemplate
  public void testBatchGetPartialResults() throws IOException {
    // Setup
    String metalakeName = "metalake_for_partial_batch";
    String catalogName = "catalog_for_partial_batch";
    String schemaName = "schema_for_partial_batch";
    createParentEntities(metalakeName, catalogName, schemaName, AUDIT_INFO);

    // Create only two tables
    TableEntity table1 =
        createTableEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofTable(metalakeName, catalogName, schemaName),
            "table1",
            AUDIT_INFO);
    TableEntity table2 =
        createTableEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofTable(metalakeName, catalogName, schemaName),
            "table2",
            AUDIT_INFO);

    backend.insert(table1, false);
    backend.insert(table2, false);

    // Request three tables, but only two exist
    List<NameIdentifier> identifiers =
        Lists.newArrayList(
            table1.nameIdentifier(),
            table2.nameIdentifier(),
            NameIdentifier.of(metalakeName, catalogName, schemaName, "nonexistent_table"));

    List<TableEntity> result = backend.batchGet(identifiers, Entity.EntityType.TABLE);

    // Should return only the existing tables
    Assertions.assertEquals(2, result.size());
    Map<String, TableEntity> resultMap =
        result.stream().collect(Collectors.toMap(TableEntity::name, t -> t));
    Assertions.assertTrue(resultMap.containsKey("table1"));
    Assertions.assertTrue(resultMap.containsKey("table2"));
    Assertions.assertFalse(resultMap.containsKey("nonexistent_table"));
  }

  @TestTemplate
  public void testBatchGetSingleItem() throws IOException {
    // Setup
    String metalakeName = "metalake_for_single_batch";
    createAndInsertMakeLake(metalakeName);

    CatalogEntity catalog =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofCatalog(metalakeName),
            "catalog_single",
            AUDIT_INFO);
    backend.insert(catalog, false);

    // Test batch get with single item
    List<NameIdentifier> identifiers = Lists.newArrayList(catalog.nameIdentifier());

    List<CatalogEntity> result = backend.batchGet(identifiers, Entity.EntityType.CATALOG);

    Assertions.assertEquals(1, result.size());
    Assertions.assertEquals("catalog_single", result.get(0).name());
  }
}
