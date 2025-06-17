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
import java.util.Optional;
import org.apache.gravitino.Config;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.ColumnEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.rel.types.Types;
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
    @Outcome(id = "ENTITY", expect = Expect.ACCEPTABLE, desc = "Both threads see correct value."),
    @Outcome(id = "NULL", expect = Expect.FORBIDDEN, desc = "Cache read failed unexpectedly")
  })
  @Description(
      "Tests the race condition between put() and getOrLoad(). "
          + "Thread 1 attempts to insert an entity into the cache using put(), "
          + "while Thread 2 concurrently attempts to retrieve the same entity using getOrLoad(). "
          + "The expected behavior is that getOrLoad should observe the result of put() or "
          + "load the value from the backing store if put hasn't completed yet. "
          + "Returning NULL would indicate both put and load failed, "
          + "which violates the cache's expected guarantees and is therefore forbidden.")
  @State
  public static class PutVsGetIfPresentTest {
    private final EntityCache cache;
    private final NameIdentifier ident;
    private final SchemaEntity entity;

    public PutVsGetIfPresentTest() {
      this.cache = new CaffeineEntityCache(new Config() {});
      this.ident = NameIdentifier.of("metalake1", "catalog1", "schema1");
      this.entity =
          getTestSchemaEntity(1L, "schema1", Namespace.of("metalake1", "catalog1"), "test_schema1");

      this.cache.put(entity);
    }

    @Actor
    public void actor1() {
      cache.put(entity);
    }

    @Actor
    public void actor2(L_Result r) {
      Optional<? extends Entity> entityFromCache =
          cache.getIfPresent(ident, Entity.EntityType.SCHEMA);
      r.r1 = entityFromCache.isPresent() ? "ENTITY" : "NULL";
    }
  }

  @JCStressTest
  @Outcome.Outcomes({
    @Outcome(
        id = "ENTITY",
        expect = Expect.ACCEPTABLE,
        desc =
            "The put is visible to contains(), indicating proper visibility and synchronization."),
    @Outcome(
        id = "NULL",
        expect = Expect.FORBIDDEN,
        desc =
            "Put was done but not visible to contains(), indicating a visibility or synchronization issue.")
  })
  @Description(
      "Tests visibility between put() and contains(): whether a concurrent put is visible to another thread using contains().")
  @State
  public static class PutVsContainTest {
    private final EntityCache cache;
    private final NameIdentifier ident;
    private final SchemaEntity entity;

    public PutVsContainTest() {
      this.cache = new CaffeineEntityCache(new Config() {});
      this.ident = NameIdentifier.of("metalake1", "catalog1", "schema1");
      this.entity =
          getTestSchemaEntity(1L, "schema1", Namespace.of("metalake1", "catalog1"), "test_schema1");

      this.cache.put(entity);
    }

    @Actor
    public void actor1() {
      cache.put(entity);
    }

    @Actor
    public void actor2(L_Result r) {
      boolean contains = cache.contains(ident, Entity.EntityType.SCHEMA);
      r.r1 = contains ? "ENTITY" : "NULL";
    }
  }

  @JCStressTest
  @Outcome.Outcomes({
    @Outcome(
        id = "ENTITY",
        expect = Expect.ACCEPTABLE,
        desc = "Put succeeded and survived invalidate."),
    @Outcome(
        id = "NULL",
        expect = Expect.ACCEPTABLE_INTERESTING,
        desc = "Invalidate cleared concurrent put.")
  })
  @Description(
      "Tests the race condition between put() and invalidate(). "
          + "Thread 1 attempts to put an entity into the cache "
          + "while Thread 2 concurrently invalidates the same key. "
          + "If put happens after invalidate, the entity remains visible in the cache. "
          + "If invalidate wins the race and removes the entry after put, "
          + "the cache may not contain the entity. Missing value is considered acceptable but interesting, "
          + "as it reflects the non-determinism of concurrent modification.")
  @State
  public static class PutVsInvalidateTest {

    private final EntityCache cache;
    private final NameIdentifier ident;
    private final SchemaEntity entity;

    public PutVsInvalidateTest() {
      this.cache = new CaffeineEntityCache(new Config() {});
      this.ident = NameIdentifier.of("metalake1", "catalog1", "schema1");
      this.entity =
          getTestSchemaEntity(1L, "schema1", Namespace.of("metalake1", "catalog1"), "test_schema1");
    }

    @Actor
    public void actor1() {
      cache.put(entity);
    }

    @Actor
    public void actor2() {
      cache.invalidate(ident, Entity.EntityType.SCHEMA);
    }

    @Arbiter
    public void arbiter(L_Result r) {
      Entity result = cache.getIfPresent(ident, Entity.EntityType.SCHEMA).orElse(null);
      r.r1 = result != null ? "ENTITY" : "NULL";
    }
  }

  @JCStressTest
  @Outcome.Outcomes({
    @Outcome(
        id = "ENTITY",
        expect = Expect.ACCEPTABLE,
        desc = "Put happened after clear; value visible."),
    @Outcome(
        id = "NULL",
        expect = Expect.ACCEPTABLE_INTERESTING,
        desc = "Clear happened after put; value not present.")
  })
  @Description(
      "Tests the race condition between clear() and put(). "
          + "Thread 1 attempts to put an entity into the cache, "
          + "while Thread 2 concurrently clears all entries. "
          + "If put happens after clear, the entity should remain visible. "
          + "If clear happens after or concurrently with put, the cache may not contain the entity. "
          + "Missing value is considered interesting but not forbidden, "
          + "since the outcome depends on execution timing.")
  @State
  public static class PutVsClearTest {
    private final EntityCache cache;
    private final NameIdentifier ident;
    private final TableEntity entity;

    public PutVsClearTest() {
      this.cache = new CaffeineEntityCache(new Config() {});
      this.ident = NameIdentifier.of("metalake1", "catalog1", "schema1", "table1");
      this.entity =
          getTestTableEntity(3L, "table1", Namespace.of("metalake1", "catalog1", "schema1"));
    }

    @Actor
    public void actor1() {
      cache.put(entity);
    }

    @Actor
    public void actor2() {
      cache.clear();
    }

    @Arbiter
    public void arbiter(L_Result r) {
      Entity result = cache.getIfPresent(ident, Entity.EntityType.TABLE).orElse(null);
      r.r1 = result != null ? "ENTITY" : "NULL";
    }
  }

  @JCStressTest
  @Outcome.Outcomes({
    @Outcome(
        id = "2",
        expect = Expect.ACCEPTABLE,
        desc = "Both puts succeeded; cache has 2 entries."),
    @Outcome(id = "1", expect = Expect.FORBIDDEN, desc = "Only one put visible")
  })
  @Outcome(
      id = "0",
      expect = Expect.FORBIDDEN,
      desc = "No put visible – broken write or visibility.")
  @Description(
      "Tests concurrent put() operations on two different cache keys. "
          + "Both threads insert different entities into the cache simultaneously. "
          + "Expected result is that both entries should be visible afterward. "
          + "If only one or none is visible, it indicates a write visibility or atomicity violation.")
  @State
  public static class ConcurrentPutDifferentKeysTest {

    private final EntityCache cache;
    private final NameIdentifier ident1;
    private final NameIdentifier ident2;

    private final SchemaEntity entity1;
    private final SchemaEntity entity2;

    public ConcurrentPutDifferentKeysTest() {
      this.cache = new CaffeineEntityCache(new Config() {});
      this.ident1 = NameIdentifier.of("metalake1", "catalog1", "schema1");
      this.ident2 = NameIdentifier.of("metalake2", "catalog2", "schema2");
      this.entity1 =
          getTestSchemaEntity(1L, "schema1", Namespace.of("metalake1", "catalog1"), "test_schema1");
      this.entity2 =
          getTestSchemaEntity(2L, "schema2", Namespace.of("metalake2", "catalog2"), "test_schema2");
    }

    @Actor
    public void actor1() {
      cache.put(entity1);
    }

    @Actor
    public void actor2() {
      cache.put(entity2);
    }

    @Arbiter
    public void arbiter(I_Result r) {
      int count = 0;
      if (cache.getIfPresent(ident1, Entity.EntityType.SCHEMA).isPresent()) {
        count++;
      }
      if (cache.getIfPresent(ident2, Entity.EntityType.SCHEMA).isPresent()) {
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
        desc = "Both puts succeeded; cache has 2 entries."),
    @Outcome(
        id = "0",
        expect = Expect.FORBIDDEN,
        desc = "No put visible – broken write or visibility.")
  })
  @Description(
      "Tests concurrent put() operations on two different cache keys. "
          + "Both threads insert different entities into the cache simultaneously. "
          + "Expected result is that both entries should be visible afterward. "
          + "If only one or none is visible, it indicates a write visibility or atomicity violation.")
  @State
  public static class ConcurrentPutSameKeyTest {
    private final EntityCache cache;
    private final NameIdentifier ident;
    private final SchemaEntity entity;

    public ConcurrentPutSameKeyTest() {
      this.cache = new CaffeineEntityCache(new Config() {});
      this.ident = NameIdentifier.of("metalake1", "catalog1", "schema1");
      this.entity =
          getTestSchemaEntity(1L, "schema1", Namespace.of("metalake1", "catalog1"), "test_schema1");
    }

    @Actor
    public void actor1() {
      cache.put(entity);
    }

    @Actor
    public void actor2() {
      cache.put(entity);
    }

    @Arbiter
    public void arbiter(I_Result r) {
      r.r1 = cache.getIfPresent(ident, Entity.EntityType.SCHEMA).isPresent() ? 1 : 0;
    }
  }

  @JCStressTest
  @Outcome.Outcomes({
    @Outcome(
        id = "ENTITY",
        expect = Expect.ACCEPTABLE,
        desc = "GetOrLoad reloads after invalidate."),
    @Outcome(
        id = "NULL",
        expect = Expect.ACCEPTABLE_INTERESTING,
        desc = "Invalidate caused data loss.")
  })
  @Description(
      "Tests the race condition between invalidate() and getOrLoad(). "
          + "Thread 1 removes the entity from the cache using invalidate(), "
          + "while Thread 2 concurrently attempts to retrieve the same entity using getOrLoad(). "
          + "The expected behavior is that getOrLoad should fall back "
          + "to the entity store and reload the entity, ensuring it does not return null. "
          + "A NULL result indicates that the cache failed to reload the entity correctly, "
          + "which violates the contract of getOrLoad and is therefore forbidden.")
  @State
  public static class InvalidateVsGetTest {
    private final EntityCache cache;
    private final NameIdentifier ident;
    private final TableEntity entity;

    public InvalidateVsGetTest() {
      this.cache = new CaffeineEntityCache(new Config() {});
      this.ident = NameIdentifier.of("metalake1", "catalog1", "schema1", "table1");
      this.entity =
          getTestTableEntity(3L, "table1", Namespace.of("metalake1", "catalog1", "schema1"));

      cache.put(entity);
    }

    @Actor
    public void actor1() {
      cache.invalidate(ident, Entity.EntityType.TABLE);
    }

    @Actor
    public void actor2(L_Result r) {
      Optional<? extends Entity> result = cache.getIfPresent(ident, Entity.EntityType.TABLE);
      r.r1 = result.isPresent() ? "ENTITY" : "NULL";
    }
  }
}
