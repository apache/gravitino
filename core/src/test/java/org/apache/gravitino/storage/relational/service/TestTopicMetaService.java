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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Instant;
import java.util.List;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.TopicEntity;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.mapper.TopicMetaMapper;
import org.apache.gravitino.storage.relational.po.TopicPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.gravitino.storage.relational.utils.POConverters;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

public class TestTopicMetaService extends TestJDBCBackend {
  private final String metalakeName = "metalake_for_topic_test";
  private final String catalogName = "catalog_for_topic_test";
  private final String schemaName = "schema_for_topic_test";
  private long schemaId;

  @BeforeEach
  public void prepare() throws IOException {
    createAndInsertMakeLake(metalakeName);
    createAndInsertCatalog(metalakeName, catalogName);
    SchemaEntity schema = createAndInsertSchema(metalakeName, catalogName, schemaName);
    schemaId = schema.id();
  }

  @TestTemplate
  public void testUpdateTopicMetaIsVersionCas() throws IOException {
    TopicEntity topic =
        createTopicEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofTopic(metalakeName, catalogName, schemaName),
            "cas_topic",
            AUDIT_INFO);
    backend.insert(topic, false);

    TopicEntity updated =
        createTopicEntity(
            topic.id(),
            NamespaceUtil.ofTopic(metalakeName, catalogName, schemaName),
            "cas_topic",
            AUDIT_INFO);

    try (SqlSession session =
        SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true)) {
      TopicMetaMapper mapper = session.getMapper(TopicMetaMapper.class);
      TopicPO oldPO = mapper.selectTopicMetaBySchemaIdAndName(schemaId, "cas_topic");
      Assertions.assertEquals(1L, oldPO.getCurrentVersion());

      // A write against the current version succeeds and advances the version (1 -> 2).
      TopicPO newPO = POConverters.updateTopicPOWithVersion(oldPO, updated);
      Assertions.assertEquals(2L, newPO.getCurrentVersion());
      Assertions.assertEquals(1, mapper.updateTopicMeta(newPO, oldPO));

      // A second write reusing the now-stale snapshot (version 1) matches 0 rows: version CAS lost.
      TopicPO staleNewPO = POConverters.updateTopicPOWithVersion(oldPO, updated);
      Assertions.assertEquals(0, mapper.updateTopicMeta(staleNewPO, oldPO));
    }
  }

  @TestTemplate
  public void testSoftDeleteTopicIsVersionCas() throws IOException {
    TopicEntity topic =
        createTopicEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofTopic(metalakeName, catalogName, schemaName),
            "drop_cas_topic",
            AUDIT_INFO);
    backend.insert(topic, false);
    long topicId = topic.id();

    try (SqlSession session =
        SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true)) {
      TopicMetaMapper mapper = session.getMapper(TopicMetaMapper.class);

      // A drop carrying a stale version matches 0 rows and leaves the topic live.
      Assertions.assertEquals(0, mapper.softDeleteTopicMetasByTopicId(topicId, 999L));
      assertTrue(backend.exists(topic.nameIdentifier(), Entity.EntityType.TOPIC));

      // A drop carrying the current version (1) soft-deletes exactly one row.
      Assertions.assertEquals(1, mapper.softDeleteTopicMetasByTopicId(topicId, 1L));
      assertFalse(backend.exists(topic.nameIdentifier(), Entity.EntityType.TOPIC));
    }
  }

  @TestTemplate
  public void testInsertAlreadyExistsException() throws IOException {
    TopicEntity topic =
        createTopicEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofFileset(metalakeName, catalogName, schemaName),
            "topic",
            AUDIT_INFO);
    TopicEntity topicCopy =
        createTopicEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofFileset(metalakeName, catalogName, schemaName),
            "topic",
            AUDIT_INFO);
    backend.insert(topic, false);
    assertThrows(EntityAlreadyExistsException.class, () -> backend.insert(topicCopy, false));
  }

  @TestTemplate
  public void testMetaLifeCycleFromCreationToDeletion() throws IOException {
    TopicEntity topic =
        createTopicEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofFileset(metalakeName, catalogName, schemaName),
            "topic",
            AUDIT_INFO);
    backend.insert(topic, false);

    List<TopicEntity> topics = backend.list(topic.namespace(), Entity.EntityType.TOPIC, true);
    assertTrue(topics.contains(topic));

    // meta data soft delete
    backend.delete(NameIdentifierUtil.ofMetalake(metalakeName), Entity.EntityType.METALAKE, true);
    assertFalse(backend.exists(topic.nameIdentifier(), Entity.EntityType.TOPIC));

    // check legacy record after soft delete
    assertTrue(legacyRecordExistsInDB(topic.id(), Entity.EntityType.TOPIC));

    // meta data hard delete
    for (Entity.EntityType entityType : Entity.EntityType.values()) {
      backend.hardDeleteLegacyData(entityType, Instant.now().toEpochMilli() + 1000);
    }
    assertFalse(legacyRecordExistsInDB(topic.id(), Entity.EntityType.TOPIC));
  }

  @TestTemplate
  public void testUpdateTopic() throws IOException {
    TopicEntity topicWithNullComment =
        TopicEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("test_null")
            .withNamespace(NamespaceUtil.ofFileset(metalakeName, catalogName, schemaName))
            .withComment(null)
            .withProperties(null)
            .withAuditInfo(AUDIT_INFO)
            .build();
    backend.insert(topicWithNullComment, false);
    backend.update(
        topicWithNullComment.nameIdentifier(),
        Entity.EntityType.TOPIC,
        e ->
            TopicEntity.builder()
                .withId(topicWithNullComment.id())
                .withName(topicWithNullComment.name())
                .withNamespace(topicWithNullComment.namespace())
                .withComment("now has comment")
                .withProperties(topicWithNullComment.properties())
                .withAuditInfo(AUDIT_INFO)
                .build());
    TopicEntity updatedTopic =
        backend.get(topicWithNullComment.nameIdentifier(), Entity.EntityType.TOPIC);
    Assertions.assertEquals("now has comment", updatedTopic.comment());

    // test topic already exists exception
    TopicEntity topic =
        createTopicEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofFileset(metalakeName, catalogName, schemaName),
            "topic",
            AUDIT_INFO);
    TopicEntity topicCopy =
        createTopicEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofFileset(metalakeName, catalogName, schemaName),
            "topic1",
            AUDIT_INFO);
    backend.insert(topic, false);
    backend.insert(topicCopy, false);
    assertThrows(
        EntityAlreadyExistsException.class,
        () ->
            backend.update(
                topicCopy.nameIdentifier(),
                Entity.EntityType.TOPIC,
                e ->
                    createTopicEntity(topicCopy.id(), topicCopy.namespace(), "topic", AUDIT_INFO)));
  }

  @TestTemplate
  public void testGetTopicByFullQualifiedNameMalformedNamespaceThrowsNoSuchEntityException()
      throws Exception {
    Method method =
        TopicMetaService.class.getDeclaredMethod(
            "getTopicPOByFullQualifiedName", NameIdentifier.class);
    method.setAccessible(true);

    NameIdentifier malformedIdentifier =
        NameIdentifier.of(Namespace.of(metalakeName, catalogName), "topic");

    InvocationTargetException invocationTargetException =
        assertThrows(
            InvocationTargetException.class,
            () -> method.invoke(TopicMetaService.getInstance(), malformedIdentifier));

    assertInstanceOf(NoSuchEntityException.class, invocationTargetException.getCause());
  }
}
