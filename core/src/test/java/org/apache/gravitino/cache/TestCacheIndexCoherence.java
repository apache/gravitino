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

import com.googlecode.concurrenttrees.radix.ConcurrentRadixTree;
import com.googlecode.concurrenttrees.radix.RadixTree;
import com.googlecode.concurrenttrees.radix.node.concrete.DefaultCharArrayNodeFactory;
import java.util.Objects;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.utils.TestUtil;
import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.Expect;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.II_Result;
import org.openjdk.jcstress.infra.results.I_Result;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;

public class TestCacheIndexCoherence {
  @JCStressTest
  @Outcome(id = "1", expect = Expect.ACCEPTABLE, desc = "Key inserted and visible")
  @Outcome(id = "0", expect = Expect.FORBIDDEN, desc = "Key not found")
  @State
  public static class SameKeyConcurrentInsertTest {
    private final RadixTree<StoreEntityCacheKey> indexTree =
        new ConcurrentRadixTree<>(new DefaultCharArrayNodeFactory());

    private final NameIdentifier ident = NameIdentifier.of("metalake1", "catalog1", "schema1");
    private final Entity entity =
        TestUtil.getTestSchemaEntity(
            123L, "schema1", Namespace.of("metalake1", "catalog1"), "ident1");
    private final StoreEntityCacheKey key = StoreEntityCacheKey.of(ident, entity.type());
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
      StoreEntityCacheKey valueForExactKey = indexTree.getValueForExactKey(keyStr);
      r.r1 =
          (valueForExactKey != null
                  && Objects.equals(valueForExactKey, key)
                  && indexTree.size() == 1)
              ? 1
              : 0;
    }
  }

  @JCStressTest
  @Outcome(id = "1", expect = Expect.ACCEPTABLE, desc = "multiple Key inserted and visible")
  @Outcome(id = "0", expect = Expect.FORBIDDEN, desc = "some Key not found")
  @State
  public static class MultipleKeyConcurrentInsertTest {
    private final RadixTree<StoreEntityCacheKey> indexTree =
        new ConcurrentRadixTree<>(new DefaultCharArrayNodeFactory());

    private final NameIdentifier ident1 = NameIdentifier.of("metalake1", "catalog1", "schema1");
    private final Entity entity1 =
        TestUtil.getTestSchemaEntity(
            123L, "schema1", Namespace.of("metalake1", "catalog1"), "ident1");
    private final NameIdentifier ident2 = NameIdentifier.of("metalake1", "catalog1", "schema2");
    private final Entity entity2 =
        TestUtil.getTestSchemaEntity(
            456L, "schema2", Namespace.of("metalake1", "catalog1"), "ident2");
    private final StoreEntityCacheKey key1 = StoreEntityCacheKey.of(ident1, entity1.type());
    private final StoreEntityCacheKey key2 = StoreEntityCacheKey.of(ident2, entity2.type());
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
      StoreEntityCacheKey valueForExactKey1 = indexTree.getValueForExactKey(key1Str);
      StoreEntityCacheKey valueForExactKey2 = indexTree.getValueForExactKey(key2Str);
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
  @Outcome(id = "1", expect = Expect.ACCEPTABLE, desc = "Key does not exist after remove.")
  @Outcome(
      id = "0",
      expect = Expect.ACCEPTABLE_INTERESTING,
      desc = "Key still exists due to put/remove race.")
  @State
  public static class PutRemoveSameKeyTest {

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
  @Outcome(
      id = "1, 1",
      expect = Expect.ACCEPTABLE,
      desc = "Both values are visible — correct and expected.")
  @Outcome(
      id = "1, 0",
      expect = Expect.ACCEPTABLE_INTERESTING,
      desc = "Only v1 is visible — race condition.")
  @Outcome(
      id = "0, 1",
      expect = Expect.ACCEPTABLE_INTERESTING,
      desc = "Only v2 is visible — race condition.")
  @Outcome(
      id = "0, 0",
      expect = Expect.FORBIDDEN,
      desc = "No value visible — inconsistent cache state.")
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
  @Outcome(id = "1", expect = Expect.ACCEPTABLE, desc = "Get sees the value")
  @Outcome(
      id = "0",
      expect = Expect.ACCEPTABLE_INTERESTING,
      desc = "Get sees nothing due to timing")
  @State
  public static class PutAndGetTest {
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
}
