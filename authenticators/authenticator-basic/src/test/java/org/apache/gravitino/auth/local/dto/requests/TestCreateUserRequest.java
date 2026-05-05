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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.gravitino.json.JsonUtils;
import org.junit.jupiter.api.Test;

public class TestCreateUserRequest {

  @Test
  public void testSerDe() throws JsonProcessingException {
    CreateUserRequest request = new CreateUserRequest("test_user", "password");

    String serJson = JsonUtils.objectMapper().writeValueAsString(request);
    CreateUserRequest deserRequest =
        JsonUtils.objectMapper().readValue(serJson, CreateUserRequest.class);

    assertEquals(request, deserRequest);
    assertEquals("test_user", deserRequest.getUser());
    assertEquals("password", deserRequest.getPassword());
  }

  @Test
  public void testValidate() {
    assertDoesNotThrow(() -> new CreateUserRequest("test_user", "password").validate());
    assertThrows(IllegalArgumentException.class, () -> new CreateUserRequest().validate());
    assertThrows(
        IllegalArgumentException.class, () -> new CreateUserRequest("test_user", " ").validate());
  }
}
