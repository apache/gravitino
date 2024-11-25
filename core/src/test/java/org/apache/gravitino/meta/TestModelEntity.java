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
package org.apache.gravitino.meta;

import com.google.common.collect.ImmutableMap;
import java.time.Instant;
import java.util.Map;
import org.apache.gravitino.Namespace;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestModelEntity {

  @Test
  public void testModelEntityFields() {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build();
    Map<String, String> properties = ImmutableMap.of("k1", "v1");

    ModelEntity modelEntity =
        ModelEntity.builder()
            .withId(1L)
            .withName("test")
            .withComment("test comment")
            .withLatestVersion(1)
            .withNamespace(Namespace.of("m1", "c1", "s1"))
            .withProperties(properties)
            .withAuditInfo(auditInfo)
            .build();

    Assertions.assertEquals(1L, modelEntity.id());
    Assertions.assertEquals("test", modelEntity.name());
    Assertions.assertEquals("test comment", modelEntity.comment());
    Assertions.assertEquals(1, modelEntity.latestVersion());
    Assertions.assertEquals(Namespace.of("m1", "c1", "s1"), modelEntity.namespace());
    Assertions.assertEquals(properties, modelEntity.properties());
    Assertions.assertEquals(auditInfo, modelEntity.auditInfo());

    ModelEntity modelEntity2 =
        ModelEntity.builder()
            .withId(1L)
            .withName("test")
            .withLatestVersion(1)
            .withNamespace(Namespace.of("m1", "c1", "s1"))
            .withProperties(properties)
            .withAuditInfo(auditInfo)
            .build();
    Assertions.assertNull(modelEntity2.comment());

    ModelEntity modelEntity3 =
        ModelEntity.builder()
            .withId(1L)
            .withName("test")
            .withComment("test comment")
            .withLatestVersion(1)
            .withNamespace(Namespace.of("m1", "c1", "s1"))
            .withAuditInfo(auditInfo)
            .build();
    Assertions.assertNull(modelEntity3.properties());
  }

  @Test
  public void testWithoutRequiredFields() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> ModelEntity.builder().build());

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          ModelEntity.builder()
              .withId(1L)
              .withNamespace(Namespace.of("m1", "c1", "s1"))
              .withAuditInfo(
                  AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
              .build();
        });

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          ModelEntity.builder()
              .withId(1L)
              .withName("test")
              .withNamespace(Namespace.of("m1", "c1", "s1"))
              .withAuditInfo(
                  AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
              .build();
        });

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          ModelEntity.builder()
              .withId(1L)
              .withName("test")
              .withNamespace(Namespace.of("m1", "c1", "s1"))
              .withLatestVersion(1)
              .build();
        });
  }
}
