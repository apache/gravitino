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

package org.apache.gravitino.auth.local.dto.responses;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.Arrays;
import org.apache.gravitino.auth.local.dto.IdpUserDTO;
import org.apache.gravitino.json.JsonUtils;
import org.junit.jupiter.api.Test;

public class TestIdpUserResponse {

  @Test
  public void testSerDe() throws JsonProcessingException {
    IdpUserDTO user =
        IdpUserDTO.builder()
            .withName("test_user")
            .withGroups(Arrays.asList("group1", "group2"))
            .build();
    IdpUserResponse response = new IdpUserResponse(user);

    String serJson = JsonUtils.objectMapper().writeValueAsString(response);
    IdpUserResponse deserResponse =
        JsonUtils.objectMapper().readValue(serJson, IdpUserResponse.class);

    assertEquals(response, deserResponse);
    assertEquals("test_user", deserResponse.getUser().name());
    assertEquals(Arrays.asList("group1", "group2"), deserResponse.getUser().groups());
  }

  @Test
  public void testValidate() {
    IdpUserDTO user =
        IdpUserDTO.builder()
            .withName("test_user")
            .withGroups(Arrays.asList("group1", "group2"))
            .build();
    assertDoesNotThrow(() -> new IdpUserResponse(user).validate());
    assertThrows(IllegalArgumentException.class, () -> new IdpUserResponse().validate());
    assertThrows(
        IllegalArgumentException.class,
        () -> IdpUserDTO.builder().withName(" ").withGroups(Arrays.asList("group1")).build());
  }
}
