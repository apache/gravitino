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

import org.apache.gravitino.json.JsonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestBulkRequests {

  @Test
  public void testDuplicateUsernames() {
    UsernamesRequest request = new UsernamesRequest(new String[] {"user1", "user1"});

    Assertions.assertThrows(IllegalArgumentException.class, request::validate);
  }

  @Test
  public void testUsernamesRequestJsonField() throws Exception {
    UsernamesRequest request =
        JsonUtils.objectMapper()
            .readValue("{\"usernames\":[\"user1\",\"user2\"]}", UsernamesRequest.class);

    request.validate();
    Assertions.assertArrayEquals(new String[] {"user1", "user2"}, request.getUsernames());
  }

  @Test
  public void testDuplicateUsersForBulkAdd() {
    BulkUserAddRequest request =
        new BulkUserAddRequest(
            new UserAddRequest[] {
              new UserAddRequest("user1", "external1", true),
              new UserAddRequest("user1", "external2", true)
            });

    Assertions.assertThrows(IllegalArgumentException.class, request::validate);
  }

  @Test
  public void testDuplicateUserExternalIdsForBulkAdd() {
    BulkUserAddRequest request =
        new BulkUserAddRequest(
            new UserAddRequest[] {
              new UserAddRequest("user1", "external1", true),
              new UserAddRequest("user2", "external1", true)
            });

    Assertions.assertThrows(IllegalArgumentException.class, request::validate);
  }
}
