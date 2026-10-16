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
package org.apache.gravitino.meta;

import com.google.common.collect.Lists;
import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import org.apache.gravitino.Namespace;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests for {@link UserEntity} equality and hashing. */
public class TestUserEntity {

  @Test
  public void testHashCodeConsistentWithBagEquality() {
    AuditInfo audit =
        AuditInfo.builder()
            .withCreator("a")
            .withCreateTime(Instant.parse("2026-01-01T00:00:00Z"))
            .build();
    UserEntity u1 =
        UserEntity.builder()
            .withId(1L)
            .withName("user")
            .withNamespace(Namespace.of("metalake"))
            .withAuditInfo(audit)
            .withRoleNames(Lists.newArrayList("role1", "role2"))
            .withRoleIds(Lists.newArrayList(1L, 2L))
            .build();
    UserEntity u2 =
        UserEntity.builder()
            .withId(1L)
            .withName("user")
            .withNamespace(Namespace.of("metalake"))
            .withAuditInfo(audit)
            .withRoleNames(Lists.newArrayList("role2", "role1"))
            .withRoleIds(Lists.newArrayList(2L, 1L))
            .build();

    // equals() compares role names/ids as unordered collections, so equal objects
    // must hash equally — otherwise HashSet/HashMap drop them.
    Assertions.assertEquals(u1, u2);
    Assertions.assertEquals(u1.hashCode(), u2.hashCode());
    Assertions.assertTrue(new HashSet<>(Collections.singletonList(u1)).contains(u2));
  }
}
