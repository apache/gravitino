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

package org.apache.gravitino.idp.dto.requests;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.gravitino.json.JsonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestAddUserRequest {

  @Test
  public void testAddUserRequestSerDe() throws JsonProcessingException {
    AddUserRequest request = new AddUserRequest("test_user", "password");

    String serJson = JsonUtils.objectMapper().writeValueAsString(request);
    AddUserRequest deserRequest = JsonUtils.objectMapper().readValue(serJson, AddUserRequest.class);

    Assertions.assertEquals(request, deserRequest);
    Assertions.assertEquals("test_user", deserRequest.getUser());
    Assertions.assertEquals("password", deserRequest.getPassword());

    // Test with null user and password
    AddUserRequest request1 = new AddUserRequest();

    String serJson1 = JsonUtils.objectMapper().writeValueAsString(request1);
    AddUserRequest deserRequest1 =
        JsonUtils.objectMapper().readValue(serJson1, AddUserRequest.class);

    Assertions.assertEquals(request1, deserRequest1);
    Assertions.assertNull(deserRequest1.getUser());
    Assertions.assertNull(deserRequest1.getPassword());
  }

  @Test
  public void testAddUserRequestValidate() {
    Assertions.assertDoesNotThrow(() -> new AddUserRequest("test_user", "password").validate());
    Assertions.assertThrows(IllegalArgumentException.class, () -> new AddUserRequest().validate());
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> new AddUserRequest(" ", "password").validate());
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> new AddUserRequest("test_user", " ").validate());
  }

  @Test
  public void testAddUserRequestToStringDoesNotExposePassword() {
    String requestString = new AddUserRequest("test_user", "password").toString();

    Assertions.assertTrue(requestString.contains("test_user"));
    Assertions.assertFalse(requestString.contains("password="));
    Assertions.assertFalse(requestString.contains("password)"));
    Assertions.assertFalse(requestString.contains("\"password\""));
  }
}
