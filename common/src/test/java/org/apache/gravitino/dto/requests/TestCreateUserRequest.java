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

package org.apache.gravitino.dto.requests;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.gravitino.json.JsonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestCreateUserRequest {

  @Test
  public void testCreateUserRequestSerDe() throws JsonProcessingException {
    CreateUserRequest request = new CreateUserRequest("test_user", "password");

    String serJson = JsonUtils.objectMapper().writeValueAsString(request);
    CreateUserRequest deserRequest =
        JsonUtils.objectMapper().readValue(serJson, CreateUserRequest.class);

    Assertions.assertEquals(request, deserRequest);
    Assertions.assertEquals("test_user", deserRequest.getUser());
    Assertions.assertEquals("password", deserRequest.getPassword());

    // Test with null user and password
    CreateUserRequest request1 = new CreateUserRequest();

    String serJson1 = JsonUtils.objectMapper().writeValueAsString(request1);
    CreateUserRequest deserRequest1 =
        JsonUtils.objectMapper().readValue(serJson1, CreateUserRequest.class);

    Assertions.assertEquals(request1, deserRequest1);
    Assertions.assertNull(deserRequest1.getUser());
    Assertions.assertNull(deserRequest1.getPassword());
  }

  @Test
  public void testCreateUserRequestValidate() {
    Assertions.assertDoesNotThrow(() -> new CreateUserRequest("test_user", "password").validate());
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> new CreateUserRequest().validate());
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> new CreateUserRequest(" ", "password").validate());
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> new CreateUserRequest("test_user", " ").validate());
  }
}
