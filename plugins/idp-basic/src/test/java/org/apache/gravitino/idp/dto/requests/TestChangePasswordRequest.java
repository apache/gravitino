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

public class TestChangePasswordRequest {

  private static final String VALID_PASSWORD = "new_password12";

  @Test
  public void testChangePasswordRequestSerDe() throws JsonProcessingException {
    ChangePasswordRequest request = new ChangePasswordRequest(VALID_PASSWORD);

    String serJson = JsonUtils.objectMapper().writeValueAsString(request);
    ChangePasswordRequest deserRequest =
        JsonUtils.objectMapper().readValue(serJson, ChangePasswordRequest.class);

    Assertions.assertEquals(request, deserRequest);
    Assertions.assertEquals(VALID_PASSWORD, deserRequest.getNewPassword());
    Assertions.assertNull(deserRequest.getOldPassword());
    Assertions.assertTrue(serJson.contains("\"newPassword\""));

    ChangePasswordRequest withOld = new ChangePasswordRequest(VALID_PASSWORD, "old_password12");
    ChangePasswordRequest deserWithOld =
        JsonUtils.objectMapper()
            .readValue(
                JsonUtils.objectMapper().writeValueAsString(withOld), ChangePasswordRequest.class);
    Assertions.assertEquals(withOld, deserWithOld);
    Assertions.assertEquals("old_password12", deserWithOld.getOldPassword());
  }

  @Test
  public void testChangePasswordRequestValidate() {
    Assertions.assertDoesNotThrow(() -> new ChangePasswordRequest(VALID_PASSWORD).validate());
    Assertions.assertDoesNotThrow(
        () -> new ChangePasswordRequest(VALID_PASSWORD, "old_password12").validate());
    IllegalArgumentException missing =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> new ChangePasswordRequest().validate());
    Assertions.assertTrue(missing.getMessage().contains("\"newPassword\""));
    IllegalArgumentException blank =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> new ChangePasswordRequest(" ").validate());
    Assertions.assertTrue(blank.getMessage().contains("\"newPassword\""));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> new ChangePasswordRequest("short").validate());
  }

  @Test
  public void testChangePasswordRequestToStringDoesNotExposePassword() {
    String requestString = new ChangePasswordRequest(VALID_PASSWORD, "old_password12").toString();

    Assertions.assertFalse(requestString.contains(VALID_PASSWORD));
    Assertions.assertFalse(requestString.contains("old_password12"));
    Assertions.assertFalse(requestString.contains("newPassword="));
    Assertions.assertFalse(requestString.contains("oldPassword="));
  }
}
