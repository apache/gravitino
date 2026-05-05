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

package org.apache.gravitino.auth.local.dto.requests;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.Arrays;
import org.apache.gravitino.json.JsonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIdpRequests {

  @Test
  public void testCreateUserRequestSerDe() throws JsonProcessingException {
    CreateUserRequest request = new CreateUserRequest("test_user", "password");

    String serJson = JsonUtils.objectMapper().writeValueAsString(request);
    CreateUserRequest deserRequest =
        JsonUtils.objectMapper().readValue(serJson, CreateUserRequest.class);

    Assertions.assertEquals(request, deserRequest);
    Assertions.assertEquals("test_user", deserRequest.getUser());
    Assertions.assertEquals("password", deserRequest.getPassword());
  }

  @Test
  public void testCreateUserRequestValidate() {
    Assertions.assertDoesNotThrow(() -> new CreateUserRequest("test_user", "password").validate());
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> new CreateUserRequest().validate());
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> new CreateUserRequest("test_user", " ").validate());
  }

  @Test
  public void testCreateGroupRequestSerDe() throws JsonProcessingException {
    CreateGroupRequest request = new CreateGroupRequest("test_group");

    String serJson = JsonUtils.objectMapper().writeValueAsString(request);
    CreateGroupRequest deserRequest =
        JsonUtils.objectMapper().readValue(serJson, CreateGroupRequest.class);

    Assertions.assertEquals(request, deserRequest);
    Assertions.assertEquals("test_group", deserRequest.getGroup());
  }

  @Test
  public void testCreateGroupRequestValidate() {
    Assertions.assertDoesNotThrow(() -> new CreateGroupRequest("test_group").validate());
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> new CreateGroupRequest(" ").validate());
  }

  @Test
  public void testResetPasswordRequestSerDe() throws JsonProcessingException {
    ResetPasswordRequest request = new ResetPasswordRequest("new_password");

    String serJson = JsonUtils.objectMapper().writeValueAsString(request);
    ResetPasswordRequest deserRequest =
        JsonUtils.objectMapper().readValue(serJson, ResetPasswordRequest.class);

    Assertions.assertEquals(request, deserRequest);
    Assertions.assertEquals("new_password", deserRequest.getPassword());
  }

  @Test
  public void testResetPasswordRequestValidate() {
    Assertions.assertDoesNotThrow(() -> new ResetPasswordRequest("new_password").validate());
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> new ResetPasswordRequest(" ").validate());
  }

  @Test
  public void testUpdateGroupUsersRequestSerDe() throws JsonProcessingException {
    UpdateGroupUsersRequest request = new UpdateGroupUsersRequest(Arrays.asList("user1", "user2"));

    String serJson = JsonUtils.objectMapper().writeValueAsString(request);
    UpdateGroupUsersRequest deserRequest =
        JsonUtils.objectMapper().readValue(serJson, UpdateGroupUsersRequest.class);

    Assertions.assertEquals(request, deserRequest);
    Assertions.assertEquals(Arrays.asList("user1", "user2"), deserRequest.getUsers());
  }

  @Test
  public void testUpdateGroupUsersRequestValidate() {
    Assertions.assertDoesNotThrow(
        () -> new UpdateGroupUsersRequest(Arrays.asList("user1", "user2")).validate());
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> new UpdateGroupUsersRequest(null).validate());
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new UpdateGroupUsersRequest(Arrays.asList()).validate());
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new UpdateGroupUsersRequest(Arrays.asList("user1", " ")).validate());
  }
}
