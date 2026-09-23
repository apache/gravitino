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
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.OptimisticLockException;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.meta.TopicEntity;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.mapper.SchemaMetaMapper;
import org.apache.gravitino.storage.relational.mapper.TopicMetaMapper;
import org.apache.gravitino.storage.relational.po.SchemaPO;
import org.apache.gravitino.storage.relational.po.TopicPO;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.function.Executable;

public class TestTopicMetaService extends TestJDBCBackend {
  private final String metalakeName = "metalake_for_topic_test";
  private final String catalogName = "catalog_for_topic_test";
  private final String schemaName = "schema_for_topic_test";
  private SchemaEntity schema;

  @BeforeEach
  public void prepare() throws IOException {
    createAndInsertMakeLake(metalakeName);
    createAndInsertCatalog(metalakeName, catalogName);
    schema = createAndInsertSchema(metalakeName, catalogName, schemaName);
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
  public void testInsertWaitsForConcurrentSchemaDelete() throws Exception {
    SchemaPO observedSchemaPO =
        SessionUtils.getWithoutCommit(
            SchemaMetaMapper.class, mapper -> mapper.selectSchemaMetaById(schema.id()));
    TopicEntity topic = createTopic("topic_racing_schema_delete", "comment");

    Throwable insertFailure =
        runWhileSchemaDeleteUncommitted(
            observedSchemaPO, () -> TopicMetaService.getInstance().insertTopic(topic, false));

    Assertions.assertInstanceOf(NoSuchEntityException.class, insertFailure);
    Assertions.assertTrue(
        SessionUtils.getWithoutCommit(
                TopicMetaMapper.class, mapper -> mapper.listTopicPOsByTopicIds(List.of(topic.id())))
            .isEmpty());
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
  public void testAlterDetectsChangeThenChangeBack() throws IOException {
    TopicEntity topic = createTopic("topic_change_back", "original");
    TopicMetaService.getInstance().insertTopic(topic, false);
    TopicPO initialPO = getTopicPO(topic.id());

    assertThrows(
        OptimisticLockException.class,
        () ->
            TopicMetaService.getInstance()
                .updateTopic(
                    topic.nameIdentifier(),
                    entity -> {
                      // Restore the original payload through a second committed alter. A full-row
                      // comparison would miss both writes, but the OCC version must still expose
                      // the stale outer update.
                      updateTopicUnchecked(
                          topic.nameIdentifier(),
                          current -> copyTopic(current, current.name(), "temporary"));
                      updateTopicUnchecked(
                          topic.nameIdentifier(),
                          current -> copyTopic(current, current.name(), "original"));
                      TopicEntity stale = (TopicEntity) entity;
                      return copyTopic(stale, stale.name(), "stale update");
                    }));

    TopicEntity stored =
        TopicMetaService.getInstance().getTopicByIdentifier(topic.nameIdentifier());
    TopicPO currentPO = getTopicPO(topic.id());
    Assertions.assertEquals("original", stored.comment());
    Assertions.assertEquals(
        initialPO.getCurrentVersion() + 2, currentPO.getCurrentVersion().longValue());
    Assertions.assertEquals(currentPO.getCurrentVersion(), currentPO.getLastVersion());
  }

  @TestTemplate
  public void testOverwriteAdvancesVersionAndRejectsStaleAlter() throws IOException {
    TopicEntity topic = createTopic("topic_overwrite_occ", "original");
    TopicMetaService.getInstance().insertTopic(topic, false);
    TopicPO initialPO = getTopicPO(topic.id());
    TopicEntity replacement = copyTopic(topic, topic.name(), "overwrite winner");

    assertThrows(
        OptimisticLockException.class,
        () ->
            TopicMetaService.getInstance()
                .updateTopic(
                    topic.nameIdentifier(),
                    entity -> {
                      insertTopicUnchecked(replacement, true);
                      TopicEntity stale = (TopicEntity) entity;
                      return copyTopic(stale, stale.name(), "stale alter");
                    }));

    TopicEntity stored =
        TopicMetaService.getInstance().getTopicByIdentifier(topic.nameIdentifier());
    TopicPO currentPO = getTopicPO(topic.id());
    Assertions.assertEquals("overwrite winner", stored.comment());
    Assertions.assertEquals(
        initialPO.getCurrentVersion() + 1, currentPO.getCurrentVersion().longValue());
    Assertions.assertEquals(currentPO.getCurrentVersion(), currentPO.getLastVersion());
  }

  @TestTemplate
  public void testNaturalKeyOverwritePreservesStoredTopicId() throws IOException {
    // Every backend resolves overwrite by the identifier and keeps the ID already stored for it.
    // Relationships keyed by that ID must remain attached to the same topic.
    TopicEntity topic = createTopic("topic_natural_key_overwrite", "original");
    TopicMetaService.getInstance().insertTopic(topic, false);
    TopicPO initialPO = getTopicPO(topic.id());
    TopicEntity replacement =
        TopicEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName(topic.name())
            .withNamespace(topic.namespace())
            .withComment("replacement")
            .withProperties(topic.properties())
            .withAuditInfo(topic.auditInfo())
            .build();

    TopicMetaService.getInstance().insertTopic(replacement, true);

    TopicEntity stored =
        TopicMetaService.getInstance().getTopicByIdentifier(topic.nameIdentifier());
    TopicPO currentPO = getTopicPO(topic.id());
    Assertions.assertEquals(topic.id(), stored.id());
    Assertions.assertEquals("replacement", stored.comment());
    Assertions.assertEquals(
        initialPO.getCurrentVersion() + 1, currentPO.getCurrentVersion().longValue());
  }

  @TestTemplate
  public void testOverwriteAdvancesBeyondBothVersionMarkers() throws IOException {
    TopicEntity topic = createTopic("topic_overwrite_mismatched_versions", "original");
    TopicMetaService.getInstance().insertTopic(topic, false);
    TopicPO initialPO = getTopicPO(topic.id());
    TopicPO inconsistentLegacyPO = copyTopicPOWithVersions(initialPO, 3L, 5L);
    SessionUtils.doWithCommit(
        TopicMetaMapper.class,
        mapper ->
            Assertions.assertEquals(1, mapper.updateTopicMeta(inconsistentLegacyPO, initialPO)));

    TopicEntity replacement = copyTopic(topic, topic.name(), "replacement");
    TopicMetaService.getInstance().insertTopic(replacement, true);

    TopicPO currentPO = getTopicPO(topic.id());
    Assertions.assertEquals(6L, currentPO.getCurrentVersion());
    Assertions.assertEquals(6L, currentPO.getLastVersion());
    Assertions.assertEquals(
        "replacement",
        TopicMetaService.getInstance().getTopicByIdentifier(topic.nameIdentifier()).comment());
  }

  @TestTemplate
  public void testAlterReportsNoSuchWhenDeletedConcurrently() throws IOException {
    TopicEntity topic = createTopic("topic_alter_deleted", "original");
    TopicMetaService.getInstance().insertTopic(topic, false);

    assertThrows(
        NoSuchEntityException.class,
        () ->
            TopicMetaService.getInstance()
                .updateTopic(
                    topic.nameIdentifier(),
                    entity -> {
                      TopicMetaService.getInstance().deleteTopic(topic.nameIdentifier());
                      TopicEntity stale = (TopicEntity) entity;
                      return copyTopic(stale, stale.name(), "stale alter");
                    }));

    assertThrows(
        NoSuchEntityException.class,
        () -> TopicMetaService.getInstance().getTopicByIdentifier(topic.nameIdentifier()));
  }

  @TestTemplate
  public void testAlterReportsNoSuchWhenRenamedConcurrently() throws IOException {
    TopicEntity topic = createTopic("topic_rename_conflict", "original");
    TopicMetaService.getInstance().insertTopic(topic, false);
    String renamedName = topic.name() + "_winner";
    NameIdentifier renamedIdentifier = NameIdentifier.of(topic.namespace(), renamedName);

    assertThrows(
        NoSuchEntityException.class,
        () ->
            TopicMetaService.getInstance()
                .updateTopic(
                    topic.nameIdentifier(),
                    entity -> {
                      updateTopicUnchecked(
                          topic.nameIdentifier(),
                          current -> copyTopic(current, renamedName, "rename winner"));
                      TopicEntity stale = (TopicEntity) entity;
                      return copyTopic(stale, stale.name(), "stale alter");
                    }));

    assertThrows(
        NoSuchEntityException.class,
        () -> TopicMetaService.getInstance().getTopicByIdentifier(topic.nameIdentifier()));
    Assertions.assertEquals(
        "rename winner",
        TopicMetaService.getInstance().getTopicByIdentifier(renamedIdentifier).comment());
  }

  @TestTemplate
  public void testStaleDeleteKeepsNewerTopicAndTagRelation() throws IOException {
    TopicEntity topic = createTopic("topic_stale_delete", "original");
    TopicMetaService.getInstance().insertTopic(topic, false);
    TagEntity tag =
        TagEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("topic_occ_tag")
            .withNamespace(NamespaceUtil.ofTag(metalakeName))
            .withAuditInfo(AUDIT_INFO)
            .build();
    TagMetaService.getInstance().insertTag(tag, false);
    TagMetaService.getInstance()
        .associateTagsWithMetadataObject(
            topic.nameIdentifier(),
            topic.type(),
            new NameIdentifier[] {tag.nameIdentifier()},
            new NameIdentifier[0]);
    TopicPO stalePO = getTopicPO(topic.id());

    TopicMetaService.getInstance()
        .updateTopic(
            topic.nameIdentifier(),
            entity -> {
              TopicEntity current = (TopicEntity) entity;
              return copyTopic(current, current.name(), "winning alter");
            });

    assertThrows(
        OptimisticLockException.class,
        () ->
            TopicMetaService.getInstance().deleteTopicWithVersion(topic.nameIdentifier(), stalePO));

    TopicEntity current =
        TopicMetaService.getInstance().getTopicByIdentifier(topic.nameIdentifier());
    Assertions.assertEquals("winning alter", current.comment());
    Assertions.assertEquals(
        List.of(tag),
        TagMetaService.getInstance()
            .listTagsForMetadataObject(topic.nameIdentifier(), topic.type()));

    assertTrue(TopicMetaService.getInstance().deleteTopic(topic.nameIdentifier()));
    assertTrue(
        TagMetaService.getInstance()
            .listAssociatedMetadataObjectsForTag(tag.nameIdentifier())
            .isEmpty());
  }

  @TestTemplate
  public void testDeleteReportsNoSuchWhenDeletedConcurrently() throws IOException {
    TopicEntity topic = createTopic("topic_double_delete", "original");
    TopicMetaService.getInstance().insertTopic(topic, false);
    TopicPO stalePO = getTopicPO(topic.id());

    TopicMetaService.getInstance().deleteTopic(topic.nameIdentifier());

    assertThrows(
        NoSuchEntityException.class,
        () ->
            TopicMetaService.getInstance().deleteTopicWithVersion(topic.nameIdentifier(), stalePO));
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

  private TopicEntity createTopic(String name, String comment) {
    return TopicEntity.builder()
        .withId(RandomIdGenerator.INSTANCE.nextId())
        .withName(name)
        .withNamespace(NamespaceUtil.ofTopic(metalakeName, catalogName, schemaName))
        .withComment(comment)
        .withProperties(Map.of("key", "value"))
        .withAuditInfo(AUDIT_INFO)
        .build();
  }

  /**
   * Holds an uncommitted schema delete open and runs {@code victim} while the schema row is locked.
   *
   * <p>The victim must wait for the delete to commit. It can then report the missing parent without
   * leaving an active topic below that deleted schema.
   */
  private Throwable runWhileSchemaDeleteUncommitted(SchemaPO observedSchemaPO, Executable victim)
      throws Exception {
    CountDownLatch schemaDeleteLocked = new CountDownLatch(1);
    CountDownLatch allowDeleteCommit = new CountDownLatch(1);
    CountDownLatch victimStarted = new CountDownLatch(1);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    Future<Throwable> deleteResult =
        executor.submit(
            () -> {
              try {
                SessionUtils.doMultipleWithCommit(
                    () -> {
                      int deleted =
                          SessionUtils.getWithoutCommit(
                              SchemaMetaMapper.class,
                              mapper ->
                                  mapper.softDeleteSchemaMetaBySchemaIdAndVersion(
                                      observedSchemaPO.getSchemaId(),
                                      observedSchemaPO.getCurrentVersion()));
                      Assertions.assertEquals(1, deleted);
                      schemaDeleteLocked.countDown();
                      try {
                        assertTrue(allowDeleteCommit.await(30, TimeUnit.SECONDS));
                      } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                      }
                    });
                return null;
              } catch (Throwable throwable) {
                return throwable;
              }
            });
    try {
      assertTrue(schemaDeleteLocked.await(30, TimeUnit.SECONDS));
      Future<Throwable> victimResult =
          executor.submit(
              () -> {
                victimStarted.countDown();
                try {
                  victim.execute();
                  return null;
                } catch (Throwable throwable) {
                  return throwable;
                }
              });
      assertTrue(victimStarted.await(30, TimeUnit.SECONDS));
      assertThrows(TimeoutException.class, () -> victimResult.get(500, TimeUnit.MILLISECONDS));

      allowDeleteCommit.countDown();
      Assertions.assertNull(deleteResult.get(30, TimeUnit.SECONDS));
      return victimResult.get(30, TimeUnit.SECONDS);
    } finally {
      allowDeleteCommit.countDown();
      executor.shutdownNow();
    }
  }

  private TopicEntity copyTopic(TopicEntity source, String name, String comment) {
    return TopicEntity.builder()
        .withId(source.id())
        .withName(name)
        .withNamespace(source.namespace())
        .withComment(comment)
        .withProperties(source.properties())
        .withAuditInfo(source.auditInfo())
        .build();
  }

  private TopicPO getTopicPO(Long topicId) {
    return SessionUtils.getWithoutCommit(
        TopicMetaMapper.class, mapper -> mapper.selectTopicMetaById(topicId));
  }

  private TopicPO copyTopicPOWithVersions(TopicPO source, Long currentVersion, Long lastVersion) {
    return TopicPO.builder()
        .withTopicId(source.getTopicId())
        .withTopicName(source.getTopicName())
        .withMetalakeId(source.getMetalakeId())
        .withCatalogId(source.getCatalogId())
        .withSchemaId(source.getSchemaId())
        .withComment(source.getComment())
        .withProperties(source.getProperties())
        .withAuditInfo(source.getAuditInfo())
        .withCurrentVersion(currentVersion)
        .withLastVersion(lastVersion)
        .withDeletedAt(source.getDeletedAt())
        .build();
  }

  private void updateTopicUnchecked(
      NameIdentifier identifier, Function<TopicEntity, TopicEntity> updater) {
    try {
      TopicMetaService.getInstance().updateTopic(identifier, updater);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void insertTopicUnchecked(TopicEntity topic, boolean overwrite) {
    try {
      TopicMetaService.getInstance().insertTopic(topic, overwrite);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
