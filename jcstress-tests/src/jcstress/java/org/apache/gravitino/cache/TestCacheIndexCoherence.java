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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.googlecode.concurrenttrees.radix.ConcurrentRadixTree;
import com.googlecode.concurrenttrees.radix.RadixTree;
import com.googlecode.concurrenttrees.radix.node.concrete.DefaultCharArrayNodeFactory;
import java.time.Instant;
import java.util.Objects;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.Description;
import org.openjdk.jcstress.annotations.Expect;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.II_Result;
import org.openjdk.jcstress.infra.results.I_Result;

public class TestCacheIndexCoherence {
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

  private static AuditInfo getTestAuditInfo() {
    return AuditInfo.builder()
        .withCreator("admin")
        .withCreateTime(Instant.now())
        .withLastModifier("admin")
        .withLastModifiedTime(Instant.now())
        .build();
  }

  @JCStressTest
  @Outcome.Outcomes({
    @Outcome(id = "1", expect = Expect.ACCEPTABLE, desc = "Key inserted and visible"),
    @Outcome(id = "0", expect = Expect.FORBIDDEN, desc = "Key not found")
  })
  @Description(
      "Tests the coherence of ConcurrentRadixTree under concurrent insertions with the same key. "
          + "Both threads concurrently insert the same key-value pair into the radix tree. The expected "
          + "behavior is that the tree maintains a single mapping without data loss or duplication. A "
          + "forbidden result indicates insertion was lost or the tree became inconsistent, violating "
          + "atomicity or thread-safety guarantees.")
  @State
  public static class InsertSameKeyCoherenceTest {
    private final RadixTree<EntityCacheKey> indexTree =
        new ConcurrentRadixTree<>(new DefaultCharArrayNodeFactory());

    private final NameIdentifier ident = NameIdentifier.of("metalake1", "catalog1", "schema1");
    private final Entity entity =
        getTestSchemaEntity(123L, "schema1", Namespace.of("metalake1", "catalog1"), "ident1");
    private final EntityCacheKey key = EntityCacheKey.of(ident, entity.type());
    private final String keyStr = key.toString();

    @Actor
    public void actor1() {
      indexTree.put(keyStr, key);
    }

    @Actor
    public void actor2() {
      indexTree.put(keyStr, key);
    }

    @Arbiter
    public void arbiter(I_Result r) {
      EntityCacheKey valueForExactKey = indexTree.getValueForExactKey(keyStr);
      r.r1 =
          (valueForExactKey != null
                  && Objects.equals(valueForExactKey, key)
                  && indexTree.size() == 1)
              ? 1
              : 0;
    }
  }

  @JCStressTest
  @Outcome.Outcomes({
    @Outcome(id = "1", expect = Expect.ACCEPTABLE, desc = "Key inserted and visible"),
    @Outcome(id = "0", expect = Expect.FORBIDDEN, desc = "Key not found")
  })
  @Description(
      "Tests that ConcurrentRadixTree handles concurrent put() operations for the same key with a "
          + "relation type. Both threads insert the same key-value pair where the key includes a "
          + "relation type (ROLE_USER_REL). The test expects the tree to maintain a single entry. A "
          + "forbidden result indicates incorrect key handling or data loss under concurrency.")
  @State
  public static class InsertSameKeyWithRelationTypeCoherenceTest {
    private final RadixTree<EntityCacheKey> indexTree =
        new ConcurrentRadixTree<>(new DefaultCharArrayNodeFactory());

    private final NameIdentifier ident = NameIdentifierUtil.ofRole("metalake", "role");
    private final EntityCacheKey key =
        EntityCacheKey.of(
            ident, Entity.EntityType.ROLE, SupportsRelationOperations.Type.ROLE_USER_REL);
    private final String keyStr = key.toString();

    @Actor
    public void actor1() {
      indexTree.put(keyStr, key);
    }

    @Actor
    public void actor2() {
      indexTree.put(keyStr, key);
    }

    @Arbiter
    public void arbiter(I_Result r) {
      EntityCacheKey valueForExactKey = indexTree.getValueForExactKey(keyStr);
      r.r1 =
          (valueForExactKey != null
                  && Objects.equals(valueForExactKey, key)
                  && indexTree.size() == 1)
              ? 1
              : 0;
    }
  }

  @JCStressTest
  @Outcome.Outcomes({
    @Outcome(id = "1", expect = Expect.ACCEPTABLE, desc = "multiple Key inserted and visible"),
    @Outcome(id = "0", expect = Expect.FORBIDDEN, desc = "some Key not found")
  })
  @Description(
      "Tests that ConcurrentRadixTree maintains consistency under concurrent insertions of two "
          + "different keys. Each thread inserts a unique key-value pair into the radix tree. "
          + "Expected behavior is that both keys are stored and visible. A forbidden result "
          + "indicates data loss or broken concurrency guarantees.")
  @State
  public static class InsertMultipleKeyCoherenceTest {
    private final RadixTree<EntityCacheKey> indexTree =
        new ConcurrentRadixTree<>(new DefaultCharArrayNodeFactory());

    private final NameIdentifier ident1 = NameIdentifier.of("metalake1", "catalog1", "schema1");
    private final Entity entity1 =
        getTestSchemaEntity(123L, "schema1", Namespace.of("metalake1", "catalog1"), "ident1");
    private final NameIdentifier ident2 = NameIdentifier.of("metalake1", "catalog1", "schema2");
    private final Entity entity2 =
        getTestSchemaEntity(456L, "schema2", Namespace.of("metalake1", "catalog1"), "ident2");
    private final EntityCacheKey key1 = EntityCacheKey.of(ident1, entity1.type());
    private final EntityCacheKey key2 = EntityCacheKey.of(ident2, entity2.type());
    private final String key1Str = key1.toString();
    private final String key2Str = key2.toString();

    @Actor
    public void actor1() {
      indexTree.put(key1Str, key1);
    }

    @Actor
    public void actor2() {
      indexTree.put(key2Str, key2);
    }

    @Arbiter
    public void arbiter(I_Result r) {
      EntityCacheKey valueForExactKey1 = indexTree.getValueForExactKey(key1Str);
      EntityCacheKey valueForExactKey2 = indexTree.getValueForExactKey(key2Str);
      r.r1 =
          (valueForExactKey1 != null
                  && valueForExactKey2 != null
                  && Objects.equals(valueForExactKey1, key1)
                  && Objects.equals(valueForExactKey2, key2)
                  && indexTree.size() == 2)
              ? 1
              : 0;
    }
  }

  @JCStressTest
  @Outcome.Outcomes({
    @Outcome(id = "1", expect = Expect.ACCEPTABLE, desc = "multiple Key inserted and visible"),
    @Outcome(id = "0", expect = Expect.FORBIDDEN, desc = "some Key not found")
  })
  @Description(
      "Tests ConcurrentRadixTree under concurrent insertions of different keys with relation types. "
          + "Thread 1 inserts a key with ROLE_USER_REL, and Thread 2 inserts a key with ROLE_GROUP_REL. "
          + "Both keys share the same NameIdentifier but differ by relation type. The tree should retain "
          + "both entries. A forbidden result indicates relation type is not correctly distinguished.")
  @State
  public static class InsertMultipleKeyWithRelationTypeCoherenceTest {
    private final RadixTree<EntityCacheKey> indexTree =
        new ConcurrentRadixTree<>(new DefaultCharArrayNodeFactory());

    private final NameIdentifier ident1 = NameIdentifierUtil.ofRole("metalake", "role1");
    private final NameIdentifier ident2 = NameIdentifierUtil.ofRole("metalake", "role1");

    private final EntityCacheKey key1 =
        EntityCacheKey.of(
            ident1, Entity.EntityType.ROLE, SupportsRelationOperations.Type.ROLE_USER_REL);
    private final EntityCacheKey key2 =
        EntityCacheKey.of(
            ident2, Entity.EntityType.ROLE, SupportsRelationOperations.Type.ROLE_GROUP_REL);
    private final String key1Str = key1.toString();
    private final String key2Str = key2.toString();

    @Actor
    public void actor1() {
      indexTree.put(key1Str, key1);
    }

    @Actor
    public void actor2() {
      indexTree.put(key2Str, key2);
    }

    @Arbiter
    public void arbiter(I_Result r) {
      EntityCacheKey valueForExactKey1 = indexTree.getValueForExactKey(key1Str);
      EntityCacheKey valueForExactKey2 = indexTree.getValueForExactKey(key2Str);
      r.r1 =
          (valueForExactKey1 != null
                  && valueForExactKey2 != null
                  && Objects.equals(valueForExactKey1, key1)
                  && Objects.equals(valueForExactKey2, key2)
                  && indexTree.size() == 2)
              ? 1
              : 0;
    }
  }

  @JCStressTest
  @Outcome.Outcomes({
    @Outcome(id = "1", expect = Expect.ACCEPTABLE, desc = "Key does not exist after remove."),
    @Outcome(
        id = "0",
        expect = Expect.ACCEPTABLE_INTERESTING,
        desc = "Key still exists due to put/remove race.")
  })
  @Description(
      "Tests the race condition between put() and remove() on the same key in ConcurrentRadixTree. "
          + "Thread 1 inserts a key, while Thread 2 removes it concurrently. Depending on execution "
          + "order, the final visibility may vary. If the key is removed successfully, it's acceptable. "
          + "If the key remains due to put-after-remove, it's considered interesting but not forbidden.")
  @State
  public static class PutRemoveSameKeyCoherenceTest {

    private final RadixTree<String> indexTree =
        new ConcurrentRadixTree<>(new DefaultCharArrayNodeFactory());
    private final String key = "catalog.schema.table";

    @Actor
    public void actorPut() {
      indexTree.put(key, "v1");
    }

    @Actor
    public void actorRemove() {
      indexTree.remove(key);
    }

    @Arbiter
    public void arbiter(I_Result r) {
      String value = indexTree.getValueForExactKey(key);
      r.r1 = (value == null) ? 1 : 0;
    }
  }

  @JCStressTest
  @Outcome.Outcomes({
    @Outcome(id = "1", expect = Expect.ACCEPTABLE, desc = "Key does not exist after remove."),
    @Outcome(
        id = "0",
        expect = Expect.ACCEPTABLE_INTERESTING,
        desc = "Key still exists due to put/remove race.")
  })
  @Description(
      "Tests the race condition between put() and remove() on a key with relation type in "
          + "ConcurrentRadixTree. Thread 1 inserts a key that includes a relation type "
          + "(ROLE_USER_REL), while Thread 2 concurrently removes the same key. If remove "
          + "happens after put, the key is deleted, which is acceptable. If put happens after remove, "
          + "the key remains, which is interesting but not forbidden.")
  @State
  public static class PutRemoveSameKeyWithRelationTypeCoherenceTest {

    private final RadixTree<EntityCacheKey> indexTree =
        new ConcurrentRadixTree<>(new DefaultCharArrayNodeFactory());

    private final NameIdentifier ident = NameIdentifierUtil.ofRole("metalake", "role");
    private final EntityCacheKey key =
        EntityCacheKey.of(
            ident, Entity.EntityType.ROLE, SupportsRelationOperations.Type.ROLE_USER_REL);
    private final String keyStr = key.toString();

    @Actor
    public void actorPut() {
      indexTree.put(keyStr, key);
    }

    @Actor
    public void actorRemove() {
      indexTree.remove(keyStr);
    }

    @Arbiter
    public void arbiter(I_Result r) {
      EntityCacheKey value = indexTree.getValueForExactKey(keyStr);
      r.r1 = (value == null) ? 1 : 0;
    }
  }

  @JCStressTest
  @Outcome.Outcomes({
    @Outcome(
        id = "1, 1",
        expect = Expect.ACCEPTABLE,
        desc = "Both values are visible — correct and expected."),
    @Outcome(
        id = "1, 0",
        expect = Expect.FORBIDDEN,
        desc = "Only v1 is visible — inconsistent cache state."),
    @Outcome(
        id = "0, 1",
        expect = Expect.FORBIDDEN,
        desc = "Only v2 is visible — inconsistent cache state."),
    @Outcome(
        id = "0, 0",
        expect = Expect.FORBIDDEN,
        desc = "No value visible — inconsistent cache state.")
  })
  @Description(
      "Tests race conditions between concurrent put() operations and a prefix scan. Thread 1 inserts "
          + "'table1' and Thread 2 inserts 'table2', both under the same prefix. Arbiter performs a "
          + "prefix scan using getValuesForKeysStartingWith(). Both values should be visible. "
          + "Missing any indicates a broken visibility guarantee, which is forbidden.")
  @State
  public static class PutAndPrefixScanCoherenceTest {

    private final RadixTree<String> indexTree =
        new ConcurrentRadixTree<>(new DefaultCharArrayNodeFactory());

    private static final String PREFIX = "catalog.schema.";
    private static final String KEY1 = PREFIX + "table1";
    private static final String KEY2 = PREFIX + "table2";

    @Actor
    public void actorPut1() {
      indexTree.put(KEY1, "v1");
    }

    @Actor
    public void actorPut2() {
      indexTree.put(KEY2, "v2");
    }

    @Arbiter
    public void arbiter(II_Result r) {
      ImmutableList<String> values =
          ImmutableList.copyOf(indexTree.getValuesForKeysStartingWith(PREFIX));
      r.r1 = values.contains("v1") ? 1 : 0;
      r.r2 = values.contains("v2") ? 1 : 0;
    }
  }

  @JCStressTest
  @Outcome.Outcomes({
    @Outcome(
        id = "1, 1",
        expect = Expect.ACCEPTABLE,
        desc = "Both values are visible — correct and expected."),
    @Outcome(
        id = "1, 0",
        expect = Expect.FORBIDDEN,
        desc = "Only v1 is visible — inconsistent cache state."),
    @Outcome(
        id = "0, 1",
        expect = Expect.FORBIDDEN,
        desc = "Only v2 is visible — inconsistent cache state."),
    @Outcome(
        id = "0, 0",
        expect = Expect.FORBIDDEN,
        desc = "No value visible — inconsistent cache state.")
  })
  @Description(
      "Tests race conditions between concurrent put() operations on keys with different relation "
          + "types and a prefix-based scan. Thread 1 inserts a ROLE_USER_REL key, Thread 2 inserts "
          + "a ROLE_GROUP_REL key. The arbiter uses getValuesForKeysStartingWith() to verify both "
          + "entries are visible. Visibility of only one is a race condition and forbidden. Missing "
          + "both values indicates an inconsistent cache state and is also forbidden.")
  @State
  public static class PutAndPrefixScanWithRelationTypeCoherenceTest {

    private final RadixTree<EntityCacheKey> indexTree =
        new ConcurrentRadixTree<>(new DefaultCharArrayNodeFactory());

    private final NameIdentifier ident1 = NameIdentifierUtil.ofRole("metalake", "role1");
    private final NameIdentifier ident2 = NameIdentifierUtil.ofRole("metalake", "role2");

    private final EntityCacheKey key1 =
        EntityCacheKey.of(
            ident1, Entity.EntityType.ROLE, SupportsRelationOperations.Type.ROLE_USER_REL);
    private final EntityCacheKey key2 =
        EntityCacheKey.of(
            ident2, Entity.EntityType.ROLE, SupportsRelationOperations.Type.ROLE_GROUP_REL);
    private final String key1Str = key1.toString();
    private final String key2Str = key2.toString();

    @Actor
    public void actorPut1() {
      indexTree.put(key1Str, key1);
    }

    @Actor
    public void actorPut2() {
      indexTree.put(key2Str, key2);
    }

    @Arbiter
    public void arbiter(II_Result r) {
      ImmutableList<EntityCacheKey> values =
          ImmutableList.copyOf(indexTree.getValuesForKeysStartingWith("metalake"));
      r.r1 = values.contains(key1) ? 1 : 0;
      r.r2 = values.contains(key2) ? 1 : 0;
    }
  }

  @JCStressTest
  @Outcome.Outcomes({
    @Outcome(id = "1", expect = Expect.ACCEPTABLE, desc = "Get sees the value"),
    @Outcome(
        id = "0",
        expect = Expect.ACCEPTABLE_INTERESTING,
        desc = "Get sees nothing due to timing")
  })
  @Description(
      "Tests the race condition between put() and getValueForExactKey(). Thread 1 inserts a key-value "
          + "pair into the radix tree. Thread 2 concurrently attempts to retrieve the value for the same "
          + "key. If get() observes the effect of put(), it should return a non-null result. If not, it "
          + "may return null depending on the timing. Missing value is interesting but not forbidden.")
  @State
  public static class PutAndGetCoherenceTest {
    private final RadixTree<String> indexTree =
        new ConcurrentRadixTree<>(new DefaultCharArrayNodeFactory());
    private final String key = "catalog.schema.table";

    @Actor
    public void actorPut() {
      indexTree.put(key, "v1");
    }

    @Actor
    public void actorGet(I_Result r) {
      r.r1 = indexTree.getValueForExactKey(key) != null ? 1 : 0;
    }
  }

  @JCStressTest
  @Outcome.Outcomes({
    @Outcome(id = "1", expect = Expect.ACCEPTABLE, desc = "Get sees the value"),
    @Outcome(
        id = "0",
        expect = Expect.ACCEPTABLE_INTERESTING,
        desc = "Get sees nothing due to timing")
  })
  @Description(
      "Tests race condition between put() and getValueForExactKey() for relation-typed cache keys. "
          + "Thread 1 inserts a ROLE_USER_REL key into the radix tree. Thread 2 concurrently attempts to "
          + "retrieve the same key. If the get observes the result of the put, it returns a non-null value. "
          + "Missing value is interesting but not forbidden, depending on execution timing.")
  @State
  public static class PutAndGetWithRelationTypeCoherenceTest {
    private final RadixTree<EntityCacheKey> indexTree =
        new ConcurrentRadixTree<>(new DefaultCharArrayNodeFactory());
    private final NameIdentifier ident = NameIdentifierUtil.ofRole("metalake", "role");
    private final EntityCacheKey key =
        EntityCacheKey.of(
            ident, Entity.EntityType.ROLE, SupportsRelationOperations.Type.ROLE_USER_REL);
    private final String keyStr = key.toString();

    @Actor
    public void actorPut() {
      indexTree.put(keyStr, key);
    }

    @Actor
    public void actorGet(I_Result r) {
      r.r1 = indexTree.getValueForExactKey(keyStr) != null ? 1 : 0;
    }
  }
}
