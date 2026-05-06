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

package org.apache.gravitino.dto.responses;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.time.Instant;
import java.util.Arrays;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.dto.IdpUserDTO;
import org.apache.gravitino.json.JsonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIdpUserResponse {

  @Test
  public void testIdpUserResponseSerDe() throws JsonProcessingException {
    IdpUserDTO user =
        IdpUserDTO.builder()
            .withName("test_user")
            .withGroups(Arrays.asList("group1", "group2"))
            .withAudit(buildAudit())
            .build();
    IdpUserResponse response = new IdpUserResponse(user);

    String serJson = JsonUtils.objectMapper().writeValueAsString(response);
    IdpUserResponse deserResponse =
        JsonUtils.objectMapper().readValue(serJson, IdpUserResponse.class);

    Assertions.assertEquals(response, deserResponse);
    Assertions.assertEquals("test_user", deserResponse.getUser().name());
    Assertions.assertEquals(Arrays.asList("group1", "group2"), deserResponse.getUser().groups());
    Assertions.assertEquals("admin", deserResponse.getUser().auditInfo().creator());
  }

  @Test
  public void testIdpUserResponseValidate() {
    IdpUserDTO user =
        IdpUserDTO.builder()
            .withName("test_user")
            .withGroups(Arrays.asList("group1", "group2"))
            .withAudit(buildAudit())
            .build();
    IdpUserResponse response = new IdpUserResponse(user);
    response.validate(); // No exception thrown
  }

  @Test
  public void testIdpUserResponseException() {
    IdpUserResponse response = new IdpUserResponse();
    Assertions.assertThrows(IllegalArgumentException.class, response::validate);
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
