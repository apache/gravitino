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

import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.Arrays;
import org.apache.gravitino.auth.local.dto.IdpGroupDTO;
import org.apache.gravitino.auth.local.dto.IdpUserDTO;
import org.apache.gravitino.json.JsonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIdpResponses {

  @Test
  public void testIdpUserResponseSerDe() throws JsonProcessingException {
    IdpUserDTO user =
        IdpUserDTO.builder()
            .withName("test_user")
            .withGroups(Arrays.asList("group1", "group2"))
            .build();
    IdpUserResponse response = new IdpUserResponse(user);

    String serJson = JsonUtils.objectMapper().writeValueAsString(response);
    IdpUserResponse deserResponse =
        JsonUtils.objectMapper().readValue(serJson, IdpUserResponse.class);

    Assertions.assertEquals(response, deserResponse);
    Assertions.assertEquals("test_user", deserResponse.getUser().name());
    Assertions.assertEquals(Arrays.asList("group1", "group2"), deserResponse.getUser().groups());
  }

  @Test
  public void testIdpUserResponseValidate() {
    IdpUserDTO user =
        IdpUserDTO.builder()
            .withName("test_user")
            .withGroups(Arrays.asList("group1", "group2"))
            .build();
    Assertions.assertDoesNotThrow(() -> new IdpUserResponse(user).validate());
    Assertions.assertThrows(IllegalArgumentException.class, () -> new IdpUserResponse().validate());
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> IdpUserDTO.builder().withName(" ").withGroups(Arrays.asList("group1")).build());
  }

  @Test
  public void testIdpGroupResponseSerDe() throws JsonProcessingException {
    IdpGroupDTO group =
        IdpGroupDTO.builder()
            .withName("test_group")
            .withUsers(Arrays.asList("user1", "user2"))
            .build();
    IdpGroupResponse response = new IdpGroupResponse(group);

    String serJson = JsonUtils.objectMapper().writeValueAsString(response);
    IdpGroupResponse deserResponse =
        JsonUtils.objectMapper().readValue(serJson, IdpGroupResponse.class);

    Assertions.assertEquals(response, deserResponse);
    Assertions.assertEquals("test_group", deserResponse.getGroup().name());
    Assertions.assertEquals(Arrays.asList("user1", "user2"), deserResponse.getGroup().users());
  }

  @Test
  public void testIdpGroupResponseValidate() {
    IdpGroupDTO group =
        IdpGroupDTO.builder()
            .withName("test_group")
            .withUsers(Arrays.asList("user1", "user2"))
            .build();
    Assertions.assertDoesNotThrow(() -> new IdpGroupResponse(group).validate());
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> new IdpGroupResponse().validate());
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> IdpGroupDTO.builder().withName(" ").withUsers(Arrays.asList("user1")).build());
  }
}
