/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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

public class TestUpdateUserRequest {

  private static final String VALID_PASSWORD = "new_password12";

  @Test
  public void testUpdateUserRequestSerDe() throws JsonProcessingException {
    UpdateUserRequest request = new UpdateUserRequest(VALID_PASSWORD);

    String serJson = JsonUtils.objectMapper().writeValueAsString(request);
    UpdateUserRequest deserRequest =
        JsonUtils.objectMapper().readValue(serJson, UpdateUserRequest.class);

    Assertions.assertEquals(request, deserRequest);
    Assertions.assertEquals(VALID_PASSWORD, deserRequest.getNewPassword());
    Assertions.assertNull(deserRequest.getOldPassword());
    Assertions.assertTrue(serJson.contains("\"newPassword\""));
    Assertions.assertFalse(serJson.contains("\"password\""));

    UpdateUserRequest withOld = new UpdateUserRequest(VALID_PASSWORD, "old_password12", null);
    UpdateUserRequest deserWithOld =
        JsonUtils.objectMapper()
            .readValue(
                JsonUtils.objectMapper().writeValueAsString(withOld), UpdateUserRequest.class);
    Assertions.assertEquals(withOld, deserWithOld);
    Assertions.assertEquals("old_password12", deserWithOld.getOldPassword());

    // Test with null password
    UpdateUserRequest request1 = new UpdateUserRequest();

    String serJson1 = JsonUtils.objectMapper().writeValueAsString(request1);
    UpdateUserRequest deserRequest1 =
        JsonUtils.objectMapper().readValue(serJson1, UpdateUserRequest.class);

    Assertions.assertEquals(request1, deserRequest1);
    Assertions.assertNull(deserRequest1.getNewPassword());

    UpdateUserRequest enabledOnly = new UpdateUserRequest(null, false);
    UpdateUserRequest deserEnabled =
        JsonUtils.objectMapper()
            .readValue(
                JsonUtils.objectMapper().writeValueAsString(enabledOnly), UpdateUserRequest.class);
    Assertions.assertEquals(enabledOnly, deserEnabled);
    Assertions.assertNull(deserEnabled.getNewPassword());
    Assertions.assertFalse(deserEnabled.getEnabled());
  }

  @Test
  public void testUpdateUserRequestValidate() {
    Assertions.assertDoesNotThrow(() -> new UpdateUserRequest(VALID_PASSWORD).validate());
    Assertions.assertDoesNotThrow(() -> new UpdateUserRequest(null, false).validate());
    Assertions.assertDoesNotThrow(() -> new UpdateUserRequest(VALID_PASSWORD, true).validate());
    Assertions.assertDoesNotThrow(
        () -> new UpdateUserRequest(VALID_PASSWORD, "old_password12", null).validate());
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> new UpdateUserRequest().validate());
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> new UpdateUserRequest(" ").validate());
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> new UpdateUserRequest("short").validate());
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new UpdateUserRequest(null, "leftover-old-12", false).validate());
  }

  @Test
  public void testUpdateUserRequestToStringDoesNotExposePassword() {
    String requestString = new UpdateUserRequest(VALID_PASSWORD, "old_password12", null).toString();

    Assertions.assertFalse(requestString.contains(VALID_PASSWORD));
    Assertions.assertFalse(requestString.contains("old_password12"));
    Assertions.assertFalse(requestString.contains("newPassword="));
    Assertions.assertFalse(requestString.contains("oldPassword="));
    Assertions.assertFalse(requestString.contains("\"newPassword\""));
  }
}
