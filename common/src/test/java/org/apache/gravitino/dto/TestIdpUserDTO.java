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

package org.apache.gravitino.dto;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.time.Instant;
import java.util.Arrays;
import org.apache.gravitino.json.JsonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIdpUserDTO {

  @Test
  public void testIdpUserDTOSerDe() throws JsonProcessingException {
    IdpUserDTO userDTO =
        IdpUserDTO.builder()
            .withName("test_user")
            .withGroups(Arrays.asList("group1", "group2"))
            .withAudit(buildAudit())
            .build();

    String json = JsonUtils.objectMapper().writeValueAsString(userDTO);
    IdpUserDTO deserialized = JsonUtils.objectMapper().readValue(json, IdpUserDTO.class);

    Assertions.assertEquals(userDTO, deserialized);
    Assertions.assertEquals("test_user", deserialized.name());
    Assertions.assertEquals(Arrays.asList("group1", "group2"), deserialized.groups());
    Assertions.assertEquals("admin", deserialized.auditInfo().creator());

    // Test with default groups
    IdpUserDTO userDTO1 =
        IdpUserDTO.builder().withName("test_user").withAudit(buildAudit()).build();

    String json1 = JsonUtils.objectMapper().writeValueAsString(userDTO1);
    IdpUserDTO deserialized1 = JsonUtils.objectMapper().readValue(json1, IdpUserDTO.class);

    Assertions.assertEquals(userDTO1, deserialized1);
    Assertions.assertEquals("test_user", deserialized1.name());
    Assertions.assertTrue(deserialized1.groups().isEmpty());

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> IdpUserDTO.builder().withName(" ").withAudit(buildAudit()).build());
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> IdpUserDTO.builder().withName("test_user").build());
  }

  private AuditDTO buildAudit() {
    return AuditDTO.builder()
        .withCreator("admin")
        .withCreateTime(Instant.parse("2024-01-01T00:00:00Z"))
        .withLastModifier("admin")
        .withLastModifiedTime(Instant.parse("2024-01-01T00:00:00Z"))
        .build();
  }
}
