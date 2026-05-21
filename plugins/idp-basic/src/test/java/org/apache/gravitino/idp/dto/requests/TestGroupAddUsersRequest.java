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
import java.util.Arrays;
import org.apache.gravitino.json.JsonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestGroupAddUsersRequest {

  @Test
  public void testGroupAddUsersRequestSerDe() throws JsonProcessingException {
    GroupAddUsersRequest request = new GroupAddUsersRequest(Arrays.asList("user1", "user2"));

    String serJson = JsonUtils.objectMapper().writeValueAsString(request);
    GroupAddUsersRequest deserRequest =
        JsonUtils.objectMapper().readValue(serJson, GroupAddUsersRequest.class);

    Assertions.assertEquals(request, deserRequest);
    Assertions.assertEquals(Arrays.asList("user1", "user2"), deserRequest.getUsers());

    GroupAddUsersRequest request1 = new GroupAddUsersRequest();

    String serJson1 = JsonUtils.objectMapper().writeValueAsString(request1);
    GroupAddUsersRequest deserRequest1 =
        JsonUtils.objectMapper().readValue(serJson1, GroupAddUsersRequest.class);

    Assertions.assertEquals(request1, deserRequest1);
    Assertions.assertNull(deserRequest1.getUsers());
  }

  @Test
  public void testGroupAddUsersRequestValidate() {
    Assertions.assertDoesNotThrow(
        () -> new GroupAddUsersRequest(Arrays.asList("user1", "user2")).validate());
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> new GroupAddUsersRequest(null).validate());
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> new GroupAddUsersRequest(Arrays.asList()).validate());
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new GroupAddUsersRequest(Arrays.asList("user1", " ")).validate());
  }

  @Test
  public void testGroupAddUsersRequestValidateEmptyUser() {
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> new GroupAddUsersRequest(Arrays.asList("user1", "")).validate());

    Assertions.assertEquals(
        "\"users\" field is required and cannot contain empty user names", exception.getMessage());
  }

  @Test
  public void testGroupAddUsersRequestValidateNullUser() {
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> new GroupAddUsersRequest(Arrays.asList("user1", null)).validate());

    Assertions.assertEquals(
        "\"users\" field is required and cannot contain empty user names", exception.getMessage());
  }
}
