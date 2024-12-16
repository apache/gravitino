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
import java.util.Map;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.json.JsonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestModelDTO {

  @Test
  public void testModelSerDe() throws JsonProcessingException {
    AuditDTO audit = AuditDTO.builder().withCreator("user1").withCreateTime(Instant.now()).build();
    Map<String, String> props = ImmutableMap.of("key", "value");

    ModelDTO modelDTO =
        ModelDTO.builder()
            .withName("model_test")
            .withComment("model comment")
            .withLatestVersion(0)
            .withProperties(props)
            .withAudit(audit)
            .build();

    String serJson = JsonUtils.objectMapper().writeValueAsString(modelDTO);
    ModelDTO deserModelDTO = JsonUtils.objectMapper().readValue(serJson, ModelDTO.class);
    Assertions.assertEquals(modelDTO, deserModelDTO);

    // Test with null comment and properties
    ModelDTO modelDTO1 =
        ModelDTO.builder().withName("model_test").withLatestVersion(0).withAudit(audit).build();

    String serJson1 = JsonUtils.objectMapper().writeValueAsString(modelDTO1);
    ModelDTO deserModelDTO1 = JsonUtils.objectMapper().readValue(serJson1, ModelDTO.class);
    Assertions.assertEquals(modelDTO1, deserModelDTO1);
    Assertions.assertNull(deserModelDTO1.comment());
    Assertions.assertNull(deserModelDTO1.properties());
  }

  @Test
  public void testInvalidModelDTO() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          ModelDTO.builder().build();
        });

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          ModelDTO.builder().withName("model_test").withLatestVersion(-1).build();
        });

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          ModelDTO.builder().withName("model_test").withLatestVersion(0).build();
        });
  }
}
