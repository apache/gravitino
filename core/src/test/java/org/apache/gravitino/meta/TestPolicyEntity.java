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

import static org.apache.gravitino.policy.Policy.SUPPORTS_ALL_OBJECT_TYPES;

import com.google.common.collect.ImmutableMap;
import java.time.Instant;
import java.util.Map;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.policy.PolicyContent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestPolicyEntity {

  @Test
  public void testPolicyEntityFields() {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build();
    Map<String, String> properties = ImmutableMap.of("k1", "v1");

    ImmutableMap<String, Object> contentFields = ImmutableMap.of("target_file_size_bytes", 1000);
    Namespace namespace = Namespace.of("m1", "c1", "s1");
    PolicyContent content = PolicyContent.custom(contentFields, properties);
    PolicyEntity policyEntity =
        PolicyEntity.builder()
            .withId(1L)
            .withName("test")
            .withNamespace(namespace)
            .withComment("test comment")
            .withPolicyType("my_compaction")
            .withEnabled(false)
            .withExclusive(true)
            .withInheritable(true)
            .withSupportedObjectTypes(SUPPORTS_ALL_OBJECT_TYPES)
            .withContent(content)
            .withAuditInfo(auditInfo)
            .build();

    Assertions.assertEquals(1L, policyEntity.id());
    Assertions.assertEquals("test", policyEntity.name());
    Assertions.assertEquals(namespace, policyEntity.namespace());
    Assertions.assertEquals("test comment", policyEntity.comment());
    Assertions.assertEquals("my_compaction", policyEntity.policyType());
    Assertions.assertFalse(policyEntity.enabled());
    Assertions.assertTrue(policyEntity.exclusive());
    Assertions.assertTrue(policyEntity.inheritable());
    Assertions.assertEquals(SUPPORTS_ALL_OBJECT_TYPES, policyEntity.supportedObjectTypes());
    Assertions.assertEquals(content, policyEntity.content());
    Assertions.assertEquals(auditInfo, policyEntity.auditInfo());

    PolicyEntity policyEntity2 =
        PolicyEntity.builder()
            .withId(1L)
            .withName("test")
            .withNamespace(namespace)
            .withPolicyType("my_compaction")
            .withEnabled(false)
            .withExclusive(true)
            .withInheritable(true)
            .withSupportedObjectTypes(SUPPORTS_ALL_OBJECT_TYPES)
            .withContent(content)
            .withAuditInfo(auditInfo)
            .build();
    Assertions.assertNull(policyEntity2.comment());
  }

  @Test
  public void testWithoutRequiredFields() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> PolicyEntity.builder().build());

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            PolicyEntity.builder()
                .withId(1L)
                .withNamespace(Namespace.of("m1", "c1", "s1"))
                .withAuditInfo(
                    AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
                .build());

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            PolicyEntity.builder()
                .withId(1L)
                .withName("test")
                .withNamespace(Namespace.of("m1", "c1", "s1"))
                .withPolicyType("my_compaction")
                .withAuditInfo(
                    AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
                .build());

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            PolicyEntity.builder()
                .withId(1L)
                .withName("test")
                .withNamespace(Namespace.of("m1", "c1", "s1"))
                .withPolicyType("my_compaction")
                .withSupportedObjectTypes(SUPPORTS_ALL_OBJECT_TYPES)
                .withAuditInfo(
                    AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
                .build());
  }
}
