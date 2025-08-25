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
package org.apache.gravitino.dto.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableMap;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.json.JsonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestModelVersionDTO {

  @Test
  public void testModelVersionSerDe() throws JsonProcessingException {
    AuditDTO audit = AuditDTO.builder().withCreator("user1").withCreateTime(Instant.now()).build();
    Map<String, String> props = ImmutableMap.of("key", "value");

    ModelVersionDTO modelVersionDTO =
        ModelVersionDTO.builder()
            .withVersion(0)
            .withComment("model version comment")
            .withAliases(new String[] {"alias1", "alias2"})
            .withUris(ImmutableMap.of("n1", "u1"))
            .withProperties(props)
            .withAudit(audit)
            .build();

    String serJson = JsonUtils.objectMapper().writeValueAsString(modelVersionDTO);
    ModelVersionDTO deserModelVersionDTO =
        JsonUtils.objectMapper().readValue(serJson, ModelVersionDTO.class);

    Assertions.assertEquals(modelVersionDTO, deserModelVersionDTO);

    // Test with null aliases
    ModelVersionDTO modelVersionDTO1 =
        ModelVersionDTO.builder()
            .withVersion(0)
            .withComment("model version comment")
            .withUris(ImmutableMap.of("n1", "u1"))
            .withProperties(props)
            .withAudit(audit)
            .build();

    String serJson1 = JsonUtils.objectMapper().writeValueAsString(modelVersionDTO1);
    ModelVersionDTO deserModelVersionDTO1 =
        JsonUtils.objectMapper().readValue(serJson1, ModelVersionDTO.class);

    Assertions.assertEquals(modelVersionDTO1, deserModelVersionDTO1);
    Assertions.assertNull(deserModelVersionDTO1.aliases());

    // Test with empty aliases
    ModelVersionDTO modelVersionDTO2 =
        ModelVersionDTO.builder()
            .withVersion(0)
            .withComment("model version comment")
            .withAliases(new String[] {})
            .withUris(ImmutableMap.of("n1", "u1"))
            .withProperties(props)
            .withAudit(audit)
            .build();

    String serJson2 = JsonUtils.objectMapper().writeValueAsString(modelVersionDTO2);
    ModelVersionDTO deserModelVersionDTO2 =
        JsonUtils.objectMapper().readValue(serJson2, ModelVersionDTO.class);

    Assertions.assertEquals(modelVersionDTO2, deserModelVersionDTO2);
    Assertions.assertArrayEquals(new String[] {}, deserModelVersionDTO2.aliases());

    // Test with null comment and properties
    ModelVersionDTO modelVersionDTO3 =
        ModelVersionDTO.builder()
            .withVersion(0)
            .withUris(ImmutableMap.of("n1", "u1"))
            .withAudit(audit)
            .build();

    String serJson3 = JsonUtils.objectMapper().writeValueAsString(modelVersionDTO3);
    ModelVersionDTO deserModelVersionDTO3 =
        JsonUtils.objectMapper().readValue(serJson3, ModelVersionDTO.class);

    Assertions.assertEquals(modelVersionDTO3, deserModelVersionDTO3);
    Assertions.assertNull(deserModelVersionDTO3.comment());
    Assertions.assertNull(deserModelVersionDTO3.properties());
  }

  @Test
  public void testInvalidModelVersionDTO() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> ModelVersionDTO.builder().build());

    Assertions.assertThrows(
        IllegalArgumentException.class, () -> ModelVersionDTO.builder().withVersion(-1).build());

    Assertions.assertThrows(
        IllegalArgumentException.class, () -> ModelVersionDTO.builder().withVersion(0).build());

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> ModelVersionDTO.builder().withVersion(0).withUris(Collections.emptyMap()).build());

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            ModelVersionDTO.builder().withVersion(0).withUris(ImmutableMap.of("n1", "u1")).build());
  }
}
