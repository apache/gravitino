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

public class TestChangePasswordRequest {

  private static final String VALID_PASSWORD = "new_password12";

  @Test
  public void testChangePasswordRequestSerDe() throws JsonProcessingException {
    ChangePasswordRequest request = new ChangePasswordRequest(VALID_PASSWORD);

    String serJson = JsonUtils.objectMapper().writeValueAsString(request);
    ChangePasswordRequest deserRequest =
        JsonUtils.objectMapper().readValue(serJson, ChangePasswordRequest.class);

    Assertions.assertEquals(request, deserRequest);
    Assertions.assertEquals(VALID_PASSWORD, deserRequest.getPassword());

    // Test with null password
    ChangePasswordRequest request1 = new ChangePasswordRequest();

    String serJson1 = JsonUtils.objectMapper().writeValueAsString(request1);
    ChangePasswordRequest deserRequest1 =
        JsonUtils.objectMapper().readValue(serJson1, ChangePasswordRequest.class);

    Assertions.assertEquals(request1, deserRequest1);
    Assertions.assertNull(deserRequest1.getPassword());
  }

  @Test
  public void testChangePasswordRequestValidate() {
    Assertions.assertDoesNotThrow(() -> new ChangePasswordRequest(VALID_PASSWORD).validate());
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> new ChangePasswordRequest().validate());
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> new ChangePasswordRequest(" ").validate());
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> new ChangePasswordRequest("short").validate());
  }

  @Test
  public void testChangePasswordRequestToStringDoesNotExposePassword() {
    String requestString = new ChangePasswordRequest(VALID_PASSWORD).toString();

    Assertions.assertFalse(requestString.contains(VALID_PASSWORD));
    Assertions.assertFalse(requestString.contains("password="));
    Assertions.assertFalse(requestString.contains("\"password\""));
  }
}
