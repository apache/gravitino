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
import com.google.common.collect.Lists;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.model.ModelVersion;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestModelVersionEntity {

  @Test
  public void testModelVersionEntityFields() {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build();
    Map<String, String> properties = ImmutableMap.of("k1", "v1");
    List<String> aliases = Lists.newArrayList("alias1", "alias2");

    ModelVersionEntity modelVersionEntity =
        ModelVersionEntity.builder()
            .withModelIdentifier(NameIdentifier.of("m1", "c1", "s1", "model1"))
            .withVersion(1)
            .withComment("test comment")
            .withAliases(aliases)
            .withProperties(properties)
            .withUris(ImmutableMap.of(ModelVersion.URI_NAME_UNKNOWN, "test_uri"))
            .withAuditInfo(auditInfo)
            .build();

    Assertions.assertEquals(
        NameIdentifier.of("m1", "c1", "s1", "model1"), modelVersionEntity.modelIdentifier());
    Assertions.assertEquals(1, modelVersionEntity.version());
    Assertions.assertEquals("test comment", modelVersionEntity.comment());
    Assertions.assertEquals(aliases, modelVersionEntity.aliases());
    Assertions.assertEquals(properties, modelVersionEntity.properties());
    Assertions.assertEquals(1, modelVersionEntity.uris().size());
    Assertions.assertTrue(modelVersionEntity.uris().containsKey(ModelVersion.URI_NAME_UNKNOWN));
    Assertions.assertEquals(
        "test_uri", modelVersionEntity.uris().get(ModelVersion.URI_NAME_UNKNOWN));
    Assertions.assertEquals(auditInfo, modelVersionEntity.auditInfo());
    Assertions.assertEquals(
        Namespace.of("m1", "c1", "s1", "model1"), modelVersionEntity.namespace());
    Assertions.assertEquals("1", modelVersionEntity.name());
    Assertions.assertThrows(UnsupportedOperationException.class, modelVersionEntity::id);

    ModelVersionEntity modelVersionEntity2 =
        ModelVersionEntity.builder()
            .withModelIdentifier(NameIdentifier.of("m1", "c1", "s1", "model1"))
            .withVersion(1)
            .withAliases(aliases)
            .withProperties(properties)
            .withUris(ImmutableMap.of(ModelVersion.URI_NAME_UNKNOWN, "test_uri"))
            .withAuditInfo(auditInfo)
            .build();
    Assertions.assertNull(modelVersionEntity2.comment());

    ModelVersionEntity modelVersionEntity3 =
        ModelVersionEntity.builder()
            .withModelIdentifier(NameIdentifier.of("m1", "c1", "s1", "model1"))
            .withVersion(1)
            .withComment("test comment")
            .withAliases(aliases)
            .withUris(ImmutableMap.of(ModelVersion.URI_NAME_UNKNOWN, "test_uri"))
            .withAuditInfo(auditInfo)
            .build();
    Assertions.assertNull(modelVersionEntity3.properties());

    ModelVersionEntity modelVersionEntity4 =
        ModelVersionEntity.builder()
            .withModelIdentifier(NameIdentifier.of("m1", "c1", "s1", "model1"))
            .withVersion(1)
            .withComment("test comment")
            .withProperties(properties)
            .withUris(ImmutableMap.of(ModelVersion.URI_NAME_UNKNOWN, "test_uri"))
            .withAuditInfo(auditInfo)
            .build();
    Assertions.assertTrue(modelVersionEntity4.aliases().isEmpty());
  }

  @Test
  public void testWithoutRequiredFields() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> ModelVersionEntity.builder().build());

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            ModelVersionEntity.builder()
                .withVersion(1)
                .withAuditInfo(
                    AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
                .build());

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            ModelVersionEntity.builder()
                .withVersion(1)
                .withUris(ImmutableMap.of(ModelVersion.URI_NAME_UNKNOWN, "test_uri"))
                .build());

    Assertions.assertThrows(
        IllegalArgumentException.class, () -> ModelVersionEntity.builder().withVersion(1).build());

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> ModelVersionEntity.builder().withVersion(1).withUris(Collections.emptyMap()).build());
  }
}
