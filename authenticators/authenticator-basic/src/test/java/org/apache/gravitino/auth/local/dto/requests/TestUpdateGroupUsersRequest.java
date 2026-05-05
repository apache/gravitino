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
import java.util.Arrays;
import org.apache.gravitino.json.JsonUtils;
import org.junit.jupiter.api.Test;

public class TestUpdateGroupUsersRequest {

  @Test
  public void testSerDe() throws JsonProcessingException {
    UpdateGroupUsersRequest request = new UpdateGroupUsersRequest(Arrays.asList("user1", "user2"));

    String serJson = JsonUtils.objectMapper().writeValueAsString(request);
    UpdateGroupUsersRequest deserRequest =
        JsonUtils.objectMapper().readValue(serJson, UpdateGroupUsersRequest.class);

    assertEquals(request, deserRequest);
    assertEquals(Arrays.asList("user1", "user2"), deserRequest.getUsers());
  }

  @Test
  public void testValidate() {
    assertDoesNotThrow(
        () -> new UpdateGroupUsersRequest(Arrays.asList("user1", "user2")).validate());
    assertThrows(
        IllegalArgumentException.class, () -> new UpdateGroupUsersRequest(null).validate());
    assertThrows(
        IllegalArgumentException.class,
        () -> new UpdateGroupUsersRequest(Arrays.asList()).validate());
    assertThrows(
        IllegalArgumentException.class,
        () -> new UpdateGroupUsersRequest(Arrays.asList("user1", " ")).validate());
  }
}
