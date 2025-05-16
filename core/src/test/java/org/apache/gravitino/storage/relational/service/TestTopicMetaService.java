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

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.TopicEntity;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.po.TopicPO;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestTopicMetaService extends TestJDBCBackend {
  private static final String METALAKE_NAME = "metalake_for_topic_meta_test";

  private static final String CATALOG_NAME = "catalog_for_topic_meta_test";

  private static final String SCHEMA_NAME = "schema_for_topic_meta_test";

  private static final Namespace TOPIC_NS = Namespace.of(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME);

  private final AuditInfo auditInfo =
      AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build();

  @Test
  void testInsertAndGetTopicByID() throws IOException {
    createParentEntities(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME, auditInfo);
    Map<String, String> properties = ImmutableMap.of("k1", "v1");

    long topicID = RandomIdGenerator.INSTANCE.nextId();
    TopicEntity topicEntity =
        TopicEntity.builder()
            .withId(topicID)
            .withName("topic1")
            .withNamespace(TOPIC_NS)
            .withComment("topic comment")
            .withProperties(properties)
            .withAuditInfo(auditInfo)
            .build();

    Assertions.assertDoesNotThrow(
        () -> TopicMetaService.getInstance().insertTopic(topicEntity, false));

    TopicPO topicPOById = TopicMetaService.getInstance().getTopicPOById(topicID);
    Assertions.assertEquals(topicID, topicPOById.getTopicId());
    Assertions.assertEquals("topic1", topicPOById.getTopicName());
    Assertions.assertEquals("topic comment", topicPOById.getComment());
  }
}
