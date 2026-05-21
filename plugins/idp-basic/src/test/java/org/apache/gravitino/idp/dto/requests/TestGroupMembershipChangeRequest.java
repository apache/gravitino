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

public class TestGroupMembershipChangeRequest {

  @Test
  public void testGroupMembershipChangeRequestSerDe() throws JsonProcessingException {
    GroupMembershipChangeRequest request =
        new GroupMembershipChangeRequest(new String[] {"user1", "user2"}, new String[] {"user3"});

    String serJson = JsonUtils.objectMapper().writeValueAsString(request);
    GroupMembershipChangeRequest deserRequest =
        JsonUtils.objectMapper().readValue(serJson, GroupMembershipChangeRequest.class);

    Assertions.assertEquals(request, deserRequest);
    Assertions.assertArrayEquals(new String[] {"user1", "user2"}, deserRequest.getAdditions());
    Assertions.assertArrayEquals(new String[] {"user3"}, deserRequest.getRemovals());

    GroupMembershipChangeRequest request1 = new GroupMembershipChangeRequest();

    String serJson1 = JsonUtils.objectMapper().writeValueAsString(request1);
    GroupMembershipChangeRequest deserRequest1 =
        JsonUtils.objectMapper().readValue(serJson1, GroupMembershipChangeRequest.class);

    Assertions.assertEquals(request1, deserRequest1);
    Assertions.assertNull(deserRequest1.getAdditions());
    Assertions.assertNull(deserRequest1.getRemovals());
  }

  @Test
  public void testGroupMembershipChangeRequestValidate() {
    Assertions.assertDoesNotThrow(
        () ->
            new GroupMembershipChangeRequest(new String[] {"user1"}, new String[] {"user2"})
                .validate());
    Assertions.assertDoesNotThrow(
        () -> new GroupMembershipChangeRequest(new String[] {"user1"}, null).validate());
    Assertions.assertDoesNotThrow(
        () -> new GroupMembershipChangeRequest(null, new String[] {"user2"}).validate());
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new GroupMembershipChangeRequest(null, null).validate());
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new GroupMembershipChangeRequest(new String[] {" "}, null).validate());
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new GroupMembershipChangeRequest(null, new String[] {"user1", ""}).validate());
  }

  @Test
  public void testGroupMembershipChangeRequestValidateNullUserInAdditions() {
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> new GroupMembershipChangeRequest(new String[] {"user1", null}, null).validate());

    Assertions.assertEquals(
        "additions must not contain null or empty user names", exception.getMessage());
  }

  @Test
  public void testGroupMembershipChangeRequestValidateNullUserInRemovals() {
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> new GroupMembershipChangeRequest(null, new String[] {null}).validate());

    Assertions.assertEquals(
        "removals must not contain null or empty user names", exception.getMessage());
  }
}
