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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.apache.gravitino.Config;
import org.apache.gravitino.Entity;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.ColumnEntity;
import org.apache.gravitino.meta.GroupEntity;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.meta.UserEntity;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.utils.NamespaceUtil;
import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.Description;
import org.openjdk.jcstress.annotations.Expect;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.I_Result;
import org.openjdk.jcstress.infra.results.L_Result;

public class TestCaffeineEntityCacheCoherence {
  private static final SchemaEntity schemaEntity =
      getTestSchemaEntity(1L, "schema1", Namespace.of("metalake1", "catalog1"), "test_schema1");
  private static final TableEntity tableEntity =
      getTestTableEntity(3L, "table1", Namespace.of("metalake1", "catalog1", "schema1"));
  private static final GroupEntity groupEntity =
      getTestGroupEntity(4L, "group1", "metalake1", ImmutableList.of("role1"));
  private static final UserEntity userEntity =
      getTestUserEntity(5L, "user1", "metalake1", ImmutableList.of(6L));
  private static final RoleEntity roleEntity = getTestRoleEntity(6L, "role1", "metalake1");

  private static SchemaEntity getTestSchemaEntity(
      long id, String name, Namespace namespace, String comment) {
    return SchemaEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(namespace)
        .withAuditInfo(getTestAuditInfo())
        .withComment(comment)
        .withProperties(ImmutableMap.of())
        .build();
  }

  private static TableEntity getTestTableEntity(long id, String name, Namespace namespace) {
    return TableEntity.builder()
        .withId(id)
        .withName(name)
        .withAuditInfo(getTestAuditInfo())
        .withNamespace(namespace)
        .withColumns(ImmutableList.of(getMockColumnEntity()))
        .build();
  }

  private static RoleEntity getTestRoleEntity(long id, String name, String metalake) {
    return RoleEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(NamespaceUtil.ofRole(metalake))
        .withAuditInfo(getTestAuditInfo())
        .withSecurableObjects(ImmutableList.of())
        .build();
  }

  private static GroupEntity getTestGroupEntity(
      long id, String name, String metalake, List<String> roles) {
    return GroupEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(NamespaceUtil.ofGroup(metalake))
        .withAuditInfo(getTestAuditInfo())
        .withRoleNames(roles)
        .build();
  }

  private static UserEntity getTestUserEntity(
      long id, String name, String metalake, List<Long> roles) {
    return UserEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(NamespaceUtil.ofUser(metalake))
        .withAuditInfo(getTestAuditInfo())
        .withRoleIds(roles)
        .build();
  }

  private static AuditInfo getTestAuditInfo() {
    return AuditInfo.builder()
        .withCreator("admin")
        .withCreateTime(Instant.now())
        .withLastModifier("admin")
        .withLastModifiedTime(Instant.now())
        .build();
  }

  private static ColumnEntity getMockColumnEntity() {
    ColumnEntity mockColumn = mock(ColumnEntity.class);
    when(mockColumn.name()).thenReturn("filed1");
    when(mockColumn.dataType()).thenReturn(Types.StringType.get());
    when(mockColumn.nullable()).thenReturn(false);
    when(mockColumn.auditInfo()).thenReturn(getTestAuditInfo());

    return mockColumn;
  }

  @JCStressTest
  @Outcome.Outcomes({
    @Outcome(id = "ENTITY", expect = Expect.ACCEPTABLE, desc = "getIfPresent observed the entity."),
    @Outcome(
        id = "NULL",
        expect = Expect.FORBIDDEN,
        desc = "getIfPresent did not observe entity, which violates visibility guarantees.")
  })
  @Description(
      "Tests visibility between put() and getIfPresent() on an existing entity. "
          + "Entity should remain visible; NULL indicates broken visibility or invalidation.")
  @State
  public static class PutWithGetIfPresentCoherenceTest {
    private final EntityCache cache;

    public PutWithGetIfPresentCoherenceTest() {
      this.cache = new CaffeineEntityCache(new Config() {});
      cache.put(schemaEntity);
    }

    @Actor
    public void actor1() {
      cache.put(schemaEntity);
    }

    @Actor
    public void actor2(L_Result r) {
      r.r1 =
          cache.getIfPresent(schemaEntity.nameIdentifier(), schemaEntity.type()).isPresent()
              ? "ENTITY"
              : "NULL";
    }
  }

  @JCStressTest
  @Outcome.Outcomes({
    @Outcome(
        id = "ENTITY",
        expect = Expect.ACCEPTABLE,
        desc =
            "contains() returns true, indicating the cache retains visibility after repeated put()."),
    @Outcome(
        id = "NULL",
        expect = Expect.FORBIDDEN,
        desc =
            "contains() returned false, indicating visibility or synchronization error during repeated put().")
  })
  @Description(
      "Tests visibility with repeated put() on the same key. "
          + "Actor1 puts again; Actor2 checks contains(). "
          + "Entity should remain visible; NULL indicates a visibility issue.")
  @State
  public static class PutWithContainCoherenceTest {
    private final EntityCache cache;

    public PutWithContainCoherenceTest() {
      this.cache = new CaffeineEntityCache(new Config() {});
      cache.put(schemaEntity);
    }

    @Actor
    public void actor1() {
      cache.put(schemaEntity);
    }

    @Actor
    public void actor2(L_Result r) {
      boolean contains = cache.contains(schemaEntity.nameIdentifier(), schemaEntity.type());
      r.r1 = contains ? "ENTITY" : "NULL";
    }
  }

  @JCStressTest
  @Outcome.Outcomes({
    @Outcome(
        id = "ENTITY",
        expect = Expect.ACCEPTABLE,
        desc = "put() completed after invalidate(), so the entity remains in the cache."),
    @Outcome(
        id = "NULL",
        expect = Expect.ACCEPTABLE_INTERESTING,
        desc = "invalidate() cleared the entity after put(), so it is no longer in the cache.")
  })
  @Description(
      "Concurrent put() and invalidate() on the same key. "
          + "If put wins, entity remains; if invalidate wins, it's removed. "
          + "Both outcomes are acceptable; NULL is interesting for consistency checks.")
  @State
  public static class PutWithInvalidateCoherenceTest {
    private final EntityCache cache;

    public PutWithInvalidateCoherenceTest() {
      this.cache = new CaffeineEntityCache(new Config() {});
    }

    @Actor
    public void actor1() {
      cache.put(schemaEntity);
    }

    @Actor
    public void actor2() {
      cache.invalidate(schemaEntity.nameIdentifier(), schemaEntity.type());
    }

    @Arbiter
    public void arbiter(L_Result r) {
      Entity result =
          cache.getIfPresent(schemaEntity.nameIdentifier(), schemaEntity.type()).orElse(null);
      r.r1 = result != null ? "ENTITY" : "NULL";
    }
  }

  @JCStressTest
  @Outcome.Outcomes({
    @Outcome(
        id = "ENTITY",
        expect = Expect.ACCEPTABLE,
        desc = "put() happened after clear(), so the entity remains in the cache."),
    @Outcome(
        id = "NULL",
        expect = Expect.ACCEPTABLE_INTERESTING,
        desc = "clear() happened after or concurrently with put(), so the cache is empty.")
  })
  @Description(
      "Concurrent put() and clear(). If put wins, entity remains. If clear wins, entity is gone. "
          + "NULL is acceptable but interesting due to race timing.")
  @State
  public static class PutWithClearCoherenceTest {
    private final EntityCache cache;

    public PutWithClearCoherenceTest() {
      this.cache = new CaffeineEntityCache(new Config() {});
    }

    @Actor
    public void actor1() {
      cache.put(tableEntity);
    }

    @Actor
    public void actor2() {
      cache.clear();
    }

    @Arbiter
    public void arbiter(L_Result r) {
      Entity result =
          cache.getIfPresent(tableEntity.nameIdentifier(), tableEntity.type()).orElse(null);
      r.r1 = result != null ? "ENTITY" : "NULL";
    }
  }

  @JCStressTest
  @Outcome.Outcomes({
    @Outcome(
        id = "2",
        expect = Expect.ACCEPTABLE,
        desc = "Both put() calls succeeded; both entries are visible in the cache."),
    @Outcome(
        id = "1",
        expect = Expect.FORBIDDEN,
        desc = "Only one entry is visible; potential visibility or atomicity issue."),
    @Outcome(
        id = "0",
        expect = Expect.FORBIDDEN,
        desc =
            "Neither entry is visible; indicates a serious failure in write propagation or cache logic.")
  })
  @Description(
      "Concurrent put() on different keys. Both schema and table should be visible (result = 2). "
          + "Lower results may indicate visibility or concurrency issues.")
  @State
  public static class ConcurrentPutDifferentKeysTest {
    private final EntityCache cache;

    public ConcurrentPutDifferentKeysTest() {
      this.cache = new CaffeineEntityCache(new Config() {});
    }

    @Actor
    public void actor1() {
      cache.put(schemaEntity);
    }

    @Actor
    public void actor2() {
      cache.put(tableEntity);
    }

    @Arbiter
    public void arbiter(I_Result r) {
      int count = 0;
      if (cache.contains(schemaEntity.nameIdentifier(), schemaEntity.type())) {
        count++;
      }
      if (cache.contains(tableEntity.nameIdentifier(), tableEntity.type())) {
        count++;
      }

      r.r1 = count;
    }
  }

  @JCStressTest
  @Outcome.Outcomes({
    @Outcome(
        id = "2",
        expect = Expect.ACCEPTABLE,
        desc = "Both put() calls succeeded; both entries are visible in the cache."),
    @Outcome(
        id = "1",
        expect = Expect.FORBIDDEN,
        desc = "Only one entry is visible; potential visibility or atomicity issue."),
    @Outcome(
        id = "0",
        expect = Expect.FORBIDDEN,
        desc =
            "Neither entry is visible; indicates a serious failure in write propagation or cache logic.")
  })
  @Description("Concurrent put() with different ROLE relation types; expect both visible (2).")
  @State
  public static class ConcurrentPutDifferentKeysWithRelationTest {
    private final EntityCache cache;

    public ConcurrentPutDifferentKeysWithRelationTest() {
      this.cache = new CaffeineEntityCache(new Config() {});
    }

    @Actor
    public void actor1() {
      cache.put(
          roleEntity.nameIdentifier(),
          Entity.EntityType.ROLE,
          SupportsRelationOperations.Type.ROLE_GROUP_REL,
          ImmutableList.of(groupEntity));
    }

    @Actor
    public void actor2() {
      cache.put(
          roleEntity.nameIdentifier(),
          Entity.EntityType.ROLE,
          SupportsRelationOperations.Type.ROLE_USER_REL,
          ImmutableList.of(userEntity));
    }

    @Arbiter
    public void arbiter(I_Result r) {
      int count = 0;
      if (cache.contains(
          roleEntity.nameIdentifier(),
          Entity.EntityType.ROLE,
          SupportsRelationOperations.Type.ROLE_USER_REL)) {
        count++;
      }
      if (cache.contains(
          roleEntity.nameIdentifier(),
          Entity.EntityType.ROLE,
          SupportsRelationOperations.Type.ROLE_GROUP_REL)) {
        count++;
      }

      r.r1 = count;
    }
  }

  @JCStressTest
  @Outcome.Outcomes({
    @Outcome(
        id = "1",
        expect = Expect.ACCEPTABLE,
        desc = "Both put() calls succeeded on the same key; value is visible."),
    @Outcome(
        id = "0",
        expect = Expect.FORBIDDEN,
        desc = "Neither put() was visible; indicates a visibility or atomicity issue.")
  })
  @Description(
      "Concurrent put() on the same key; value should remain visible. Missing entry indicates a concurrency issue.")
  @State
  public static class ConcurrentPutSameKeyTest {
    private final EntityCache cache;

    public ConcurrentPutSameKeyTest() {
      this.cache = new CaffeineEntityCache(new Config() {});
    }

    @Actor
    public void actor1() {
      cache.put(schemaEntity);
    }

    @Actor
    public void actor2() {
      cache.put(schemaEntity);
    }

    @Arbiter
    public void arbiter(I_Result r) {
      r.r1 = cache.contains(schemaEntity.nameIdentifier(), Entity.EntityType.SCHEMA) ? 1 : 0;
    }
  }

  @JCStressTest
  @Outcome.Outcomes({
    @Outcome(
        id = "1",
        expect = Expect.ACCEPTABLE,
        desc = "Entry is visible; concurrent put() calls succeeded."),
    @Outcome(
        id = "0",
        expect = Expect.FORBIDDEN,
        desc = "Entry is missing; indicates visibility or atomicity issue.")
  })
  @Description(
      "Tests concurrent put() on the same key with relation type. "
          + "Entry must remain visible after concurrent writes; missing indicates a bug.")
  @State
  public static class ConcurrentPutSameKeyWithRelationTest {
    private final EntityCache cache;

    public ConcurrentPutSameKeyWithRelationTest() {
      this.cache = new CaffeineEntityCache(new Config() {});
    }

    @Actor
    public void actor1() {
      cache.put(
          roleEntity.nameIdentifier(),
          Entity.EntityType.ROLE,
          SupportsRelationOperations.Type.ROLE_USER_REL,
          ImmutableList.of(userEntity));
    }

    @Actor
    public void actor2() {
      cache.put(
          roleEntity.nameIdentifier(),
          Entity.EntityType.ROLE,
          SupportsRelationOperations.Type.ROLE_USER_REL,
          ImmutableList.of(userEntity));
    }

    @Arbiter
    public void arbiter(I_Result r) {
      r.r1 =
          cache.contains(
                  roleEntity.nameIdentifier(),
                  Entity.EntityType.ROLE,
                  SupportsRelationOperations.Type.ROLE_USER_REL)
              ? 1
              : 0;
    }
  }

  @JCStressTest
  @Outcome.Outcomes({
    @Outcome(
        id = "ENTITY",
        expect = Expect.ACCEPTABLE,
        desc = "GetIfPresent sees the entity before it was invalidated."),
    @Outcome(
        id = "NULL",
        expect = Expect.ACCEPTABLE_INTERESTING,
        desc = "Invalidate removed the entity before getIfPresent.")
  })
  @Description(
      "Tests race between invalidate() and getIfPresent(). "
          + "Either outcome is allowed depending on timing.")
  @State
  public static class InvalidateWithGetCoherenceTest {
    private final EntityCache cache;

    public InvalidateWithGetCoherenceTest() {
      this.cache = new CaffeineEntityCache(new Config() {});

      cache.put(schemaEntity);
      cache.put(tableEntity);
    }

    @Actor
    public void actor1() {
      cache.invalidate(schemaEntity.nameIdentifier(), schemaEntity.type());
    }

    @Actor
    public void actor2(L_Result r) {
      Optional<? extends Entity> result =
          cache.getIfPresent(tableEntity.nameIdentifier(), tableEntity.type());
      r.r1 = result.isPresent() ? "ENTITY" : "NULL";
    }
  }

  @JCStressTest
  @Outcome.Outcomes({
    @Outcome(
        id = "SUCCESS",
        expect = Expect.ACCEPTABLE,
        desc = "Both invalidates executed safely."),
    @Outcome(id = "FAILURE", expect = Expect.FORBIDDEN, desc = "One or both invalidates failed.")
  })
  @Description("Tests concurrent invalidate() on the same key is safe and idempotent.")
  @State
  public static class ConcurrentInvalidateSameKeyCoherenceTest {
    private final EntityCache cache;

    public ConcurrentInvalidateSameKeyCoherenceTest() {
      this.cache = new CaffeineEntityCache(new Config() {});
      cache.put(schemaEntity);
    }

    @Actor
    public void actor1() {
      cache.invalidate(schemaEntity.nameIdentifier(), schemaEntity.type());
    }

    @Actor
    public void actor2() {
      cache.invalidate(schemaEntity.nameIdentifier(), schemaEntity.type());
    }

    @Arbiter
    public void arbiter(L_Result r) {
      r.r1 =
          cache.contains(schemaEntity.nameIdentifier(), schemaEntity.type())
              ? "FAILURE"
              : "SUCCESS";
    }
  }

  @JCStressTest
  @Outcome.Outcomes({
    @Outcome(
        id = "SUCCESS",
        expect = Expect.ACCEPTABLE,
        desc = "Both invalidate operations completed; neither entry remains in the cache."),
    @Outcome(
        id = "FAILURE",
        expect = Expect.FORBIDDEN,
        desc = "One or both entries remain in the cache after concurrent invalidate.")
  })
  @Description(
      "Tests concurrent invalidate() on two related keys (same prefix). "
          + "Verifies that prefix-based invalidation logic does not interfere across keys, "
          + "and both schema and table entries are properly removed without conflict or race condition.")
  @State
  public static class ConcurrentInvalidateRelatedKeyCoherenceTest {
    private final EntityCache cache;

    public ConcurrentInvalidateRelatedKeyCoherenceTest() {
      this.cache = new CaffeineEntityCache(new Config() {});
      cache.put(schemaEntity);
      cache.put(tableEntity);
    }

    @Actor
    public void actor1() {
      cache.invalidate(schemaEntity.nameIdentifier(), schemaEntity.type());
    }

    @Actor
    public void actor2() {
      cache.invalidate(tableEntity.nameIdentifier(), tableEntity.type());
    }

    @Arbiter
    public void arbiter(L_Result r) {
      if (cache.contains(schemaEntity.nameIdentifier(), schemaEntity.type())
          || cache.contains(tableEntity.nameIdentifier(), tableEntity.type())) {
        r.r1 = "FAILURE";
        return;
      }

      r.r1 = "SUCCESS";
    }
  }

  @JCStressTest
  @Outcome.Outcomes({
    @Outcome(
        id = "SUCCESS",
        expect = Expect.ACCEPTABLE,
        desc = "Both invalidate() and clear() removed the entries as expected."),
    @Outcome(
        id = "FAILURE",
        expect = Expect.FORBIDDEN,
        desc = "One or more entries remain; indicates race condition or incomplete invalidation.")
  })
  @Description(
      "Tests concurrent invalidate() and clear() operations. "
          + "Ensures that both targeted and global removals can coexist without leaving residual entries. "
          + "Any remaining entries indicate inconsistency in concurrent removal paths.")
  @State
  public static class ClearWithInvalidateCoherenceTest {
    private final EntityCache cache;

    public ClearWithInvalidateCoherenceTest() {
      this.cache = new CaffeineEntityCache(new Config() {});
      cache.put(schemaEntity);
      cache.put(tableEntity);
    }

    @Actor
    public void actor1() {
      cache.invalidate(schemaEntity.nameIdentifier(), schemaEntity.type());
    }

    @Actor
    public void actor2() {
      cache.clear();
    }

    @Arbiter
    public void arbiter(L_Result r) {
      if (cache.contains(schemaEntity.nameIdentifier(), schemaEntity.type())
          || cache.contains(tableEntity.nameIdentifier(), tableEntity.type())) {
        r.r1 = "FAILURE";
        return;
      }

      r.r1 = "SUCCESS";
    }
  }
}
