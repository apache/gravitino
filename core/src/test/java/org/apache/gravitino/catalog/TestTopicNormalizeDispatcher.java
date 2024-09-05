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
package org.apache.gravitino.catalog;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Map;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.messaging.Topic;
import org.apache.gravitino.messaging.TopicChange;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestTopicNormalizeDispatcher extends TestOperationDispatcher {
  private static TopicNormalizeDispatcher topicNormalizeDispatcher;
  private static SchemaNormalizeDispatcher schemaNormalizeDispatcher;

  @BeforeAll
  public static void initialize() throws IOException, IllegalAccessException {
    TestTopicOperationDispatcher.initialize();
    schemaNormalizeDispatcher =
        new SchemaNormalizeDispatcher(
            TestTopicOperationDispatcher.schemaOperationDispatcher, catalogManager);
    topicNormalizeDispatcher =
        new TopicNormalizeDispatcher(
            TestTopicOperationDispatcher.topicOperationDispatcher, catalogManager);
  }

  @Test
  public void testNameCaseInsensitive() {
    Namespace topicNs = Namespace.of(metalake, catalog, "schema161");
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    schemaNormalizeDispatcher.createSchema(NameIdentifier.of(topicNs.levels()), "comment", props);

    // test case-insensitive in creation
    NameIdentifier topicIdent = NameIdentifier.of(topicNs, "topicNAME");
    Topic createdTopic = topicNormalizeDispatcher.createTopic(topicIdent, "comment", null, props);
    Assertions.assertEquals(topicIdent.name().toLowerCase(), createdTopic.name());

    // test case-insensitive in loading
    Topic loadedTopic = topicNormalizeDispatcher.loadTopic(topicIdent);
    Assertions.assertEquals(topicIdent.name().toLowerCase(), loadedTopic.name());

    // test case-insensitive in listing
    NameIdentifier[] idents = topicNormalizeDispatcher.listTopics(topicNs);
    Assertions.assertEquals(topicIdent.name().toLowerCase(), idents[0].name());

    // test case-insensitive in altering
    Topic alteredTopic =
        topicNormalizeDispatcher.alterTopic(
            NameIdentifier.of(topicNs, topicIdent.name().toLowerCase()),
            TopicChange.setProperty("k2", "v2"));
    Assertions.assertEquals(topicIdent.name().toLowerCase(), alteredTopic.name());

    // test case-insensitive in dropping
    Assertions.assertTrue(
        topicNormalizeDispatcher.dropTopic(
            NameIdentifier.of(topicNs, topicIdent.name().toUpperCase())));
    Assertions.assertFalse(topicNormalizeDispatcher.topicExists(topicIdent));
  }

  @Test
  public void testNameSpec() {
    Namespace topicNs = Namespace.of(metalake, catalog, "testNameSpec");
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    schemaNormalizeDispatcher.createSchema(NameIdentifier.of(topicNs.levels()), "comment", props);

    NameIdentifier topicIdent =
        NameIdentifier.of(topicNs, MetadataObjects.METADATA_OBJECT_RESERVED_NAME);
    Exception exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> topicNormalizeDispatcher.createTopic(topicIdent, "comment", null, props));
    Assertions.assertEquals(
        "The TOPIC name '*' is reserved. Illegal name: *", exception.getMessage());

    NameIdentifier topicIdent2 = NameIdentifier.of(topicNs, "a?");
    exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> topicNormalizeDispatcher.createTopic(topicIdent2, "comment", null, props));
    Assertions.assertEquals(
        "The TOPIC name 'a?' is illegal. Illegal name: a?", exception.getMessage());
  }
}
