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

package org.apache.gravitino;

import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestUserPrincipal {

  @Test
  public void testUserPrincipal() {
    UserPrincipal user = new UserPrincipal("user1");
    Assertions.assertEquals("user1", user.getName());
    Assertions.assertEquals(Collections.emptyList(), user.getGroups());

    List<String> groups = ImmutableList.of("g1", "g2");
    UserPrincipal userWithGroups = new UserPrincipal("user2", groups);
    Assertions.assertEquals("user2", userWithGroups.getName());
    Assertions.assertEquals(groups, userWithGroups.getGroups());

    // Check immutability of groups
    Assertions.assertThrows(
        UnsupportedOperationException.class, () -> userWithGroups.getGroups().add("g3"));

    // Check null groups
    UserPrincipal userWithNullGroups = new UserPrincipal("user3", null);
    Assertions.assertEquals("user3", userWithNullGroups.getName());
    Assertions.assertEquals(Collections.emptyList(), userWithNullGroups.getGroups());

    // Check equals and hashcode
    UserPrincipal userWithGroups2 = new UserPrincipal("user2", groups);
    Assertions.assertEquals(userWithGroups, userWithGroups2);
    Assertions.assertEquals(userWithGroups.hashCode(), userWithGroups2.hashCode());

    UserPrincipal userDifferentGroups = new UserPrincipal("user2", ImmutableList.of("g1"));
    Assertions.assertNotEquals(userWithGroups, userDifferentGroups);
  }
}
