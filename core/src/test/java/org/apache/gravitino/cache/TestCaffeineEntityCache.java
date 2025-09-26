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

package org.apache.gravitino.cache;

import static org.apache.gravitino.utils.TestUtil.getTestGroupEntity;
import static org.apache.gravitino.utils.TestUtil.getTestRoleEntity;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Optional;
import org.apache.gravitino.Config;
import org.apache.gravitino.Entity;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.meta.GroupEntity;
import org.apache.gravitino.meta.RoleEntity;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestCaffeineEntityCache {

  private static final GroupEntity groupEntity1 =
      getTestGroupEntity(1L, "group1", "metalake1", ImmutableList.of("role1"));
  private static final GroupEntity groupEntity2 =
      getTestGroupEntity(2L, "group2", "metalake1", ImmutableList.of("role1"));
  private static final RoleEntity roleEntity = getTestRoleEntity(1L, "role1", "metalake1");

  @Test
  public void testPutAndMergeWithRelationType() {
    EntityCache cache = new CaffeineEntityCache(new Config() {});

    cache.put(
        roleEntity.nameIdentifier(),
        Entity.EntityType.ROLE,
        SupportsRelationOperations.Type.ROLE_GROUP_REL,
        ImmutableList.of(groupEntity1));

    Optional<List<GroupEntity>> result1 =
        cache.getIfPresent(
            SupportsRelationOperations.Type.ROLE_GROUP_REL,
            roleEntity.nameIdentifier(),
            Entity.EntityType.ROLE);
    Assertions.assertTrue(result1.isPresent());
    Assertions.assertEquals(1, result1.get().size());
    Assertions.assertEquals("group1", result1.get().get(0).name());

    cache.put(
        roleEntity.nameIdentifier(),
        Entity.EntityType.ROLE,
        SupportsRelationOperations.Type.ROLE_GROUP_REL,
        ImmutableList.of(groupEntity2));

    Optional<List<GroupEntity>> result2 =
        cache.getIfPresent(
            SupportsRelationOperations.Type.ROLE_GROUP_REL,
            roleEntity.nameIdentifier(),
            Entity.EntityType.ROLE);
    Assertions.assertTrue(result2.isPresent());
    Assertions.assertEquals(2, result2.get().size());

    List<String> groupNames = result2.get().stream().map(GroupEntity::name).toList();
    Assertions.assertTrue(groupNames.contains("group1"));
    Assertions.assertTrue(groupNames.contains("group2"));
  }
}
