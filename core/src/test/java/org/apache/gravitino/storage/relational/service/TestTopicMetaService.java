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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.meta.TopicEntity;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

public class TestTopicMetaService extends TestJDBCBackend {
  private final String metalakeName = "metalake_for_topic_test";
  private final String catalogName = "catalog_for_topic_test";
  private final String schemaName = "schema_for_topic_test";

  @BeforeEach
  public void prepare() throws IOException {
    createAndInsertMakeLake(metalakeName);
    createAndInsertCatalog(metalakeName, catalogName);
    createAndInsertSchema(metalakeName, catalogName, schemaName);
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
  public void testUpdateAlreadyExistsException() throws IOException {
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
}
