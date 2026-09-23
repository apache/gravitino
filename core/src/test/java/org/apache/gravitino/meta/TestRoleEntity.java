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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests for {@link RoleEntity} equality and hashing. */
public class TestRoleEntity {

  @Test
  public void testHashCodeConsistentWithBagEquality() {
    AuditInfo audit =
        AuditInfo.builder()
            .withCreator("a")
            .withCreateTime(Instant.parse("2026-01-01T00:00:00Z"))
            .build();
    SecurableObject table1 =
        SecurableObjects.parse("catalog.db.table1", MetadataObject.Type.TABLE, ImmutableList.of());
    SecurableObject table2 =
        SecurableObjects.parse("catalog.db.table2", MetadataObject.Type.TABLE, ImmutableList.of());
    RoleEntity r1 =
        RoleEntity.builder()
            .withId(1L)
            .withName("role")
            .withNamespace(Namespace.of("metalake"))
            .withAuditInfo(audit)
            .withSecurableObjects(Lists.newArrayList(table1, table2))
            .build();
    RoleEntity r2 =
        RoleEntity.builder()
            .withId(1L)
            .withName("role")
            .withNamespace(Namespace.of("metalake"))
            .withAuditInfo(audit)
            .withSecurableObjects(Lists.newArrayList(table2, table1))
            .build();

    // equals() compares securable objects as an unordered collection, so equal
    // objects must hash equally — otherwise HashSet/HashMap drop them.
    Assertions.assertEquals(r1, r2);
    Assertions.assertEquals(r1.hashCode(), r2.hashCode());
    Assertions.assertTrue(new HashSet<>(Collections.singletonList(r1)).contains(r2));
  }
}
