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

package org.apache.gravitino.idp.dto.responses;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.Arrays;
import org.apache.gravitino.idp.dto.IdpGroupDTO;
import org.apache.gravitino.json.JsonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIdpGroupResponse {

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
    IdpGroupResponse response = new IdpGroupResponse(group);
    response.validate(); // No exception thrown
  }

  @Test
  public void testIdpGroupResponseException() {
    IdpGroupResponse response = new IdpGroupResponse();
    Assertions.assertThrows(IllegalArgumentException.class, response::validate);
  }

  @Test
  public void testIdpGroupResponseBlankNameMessage() throws JsonProcessingException {
    IdpGroupResponse response =
        JsonUtils.objectMapper()
            .readValue(
                "{\"code\":0,\"group\":{\"name\":\" \",\"users\":[\"user1\"]}}",
                IdpGroupResponse.class);

    IllegalArgumentException exception =
        Assertions.assertThrows(IllegalArgumentException.class, response::validate);

    Assertions.assertEquals("group 'name' must not be null or empty", exception.getMessage());
  }

  @Test
  public void testIdpGroupResponseBlankUserMessage() throws JsonProcessingException {
    IdpGroupResponse response =
        JsonUtils.objectMapper()
            .readValue(
                "{\"code\":0,\"group\":{\"name\":\"test_group\",\"users\":[\" \"]}}",
                IdpGroupResponse.class);

    IllegalArgumentException exception =
        Assertions.assertThrows(IllegalArgumentException.class, response::validate);

    Assertions.assertEquals(
        "group 'users' must not contain null or empty user names", exception.getMessage());
  }
}
