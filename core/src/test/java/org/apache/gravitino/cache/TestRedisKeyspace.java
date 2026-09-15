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
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests the key layout of {@link RedisEntityCache}. */
public class TestRedisKeyspace {

  private static final String SEP = ":";

  private final RedisKeyspace keyspace = new RedisKeyspace("ns");

  @Test
  void testHashTagIsTheMetalake() {
    Assertions.assertEquals("m1", RedisKeyspace.hashTag(NameIdentifier.of("m1")));
    Assertions.assertEquals("m1", RedisKeyspace.hashTag(NameIdentifier.of("m1", "c1")));
    Assertions.assertEquals("m1", RedisKeyspace.hashTag(NameIdentifier.of("m1", "c1", "s1", "t1")));
  }

  @Test
  void testKeysShareTheMetalakeSlotPrefix() {
    NameIdentifier metalake = NameIdentifier.of("m1");
    NameIdentifier table = NameIdentifier.of("m1", "c1", "s1", "t1");

    Assertions.assertEquals("ns:{m1}:", keyspace.slotPrefix(metalake));
    Assertions.assertEquals("ns:{m1}:", keyspace.slotPrefix(table));
    Assertions.assertEquals("ns:{m1}:IDX", keyspace.indexKey(table));
    Assertions.assertEquals(
        "ns:{m1}:D:m1.c1.s1.t1:TABLE",
        keyspace.valueKey(EntityCacheKey.of(table, Entity.EntityType.TABLE)));
    Assertions.assertEquals(
        "ns:{m1}:D:m1:METALAKE",
        keyspace.valueKey(EntityCacheKey.of(metalake, Entity.EntityType.METALAKE)));
    Assertions.assertEquals("ns:{m1}:F:m1.c1", keyspace.fenceKey(table, "m1.c1"));
  }

  @Test
  void testMemberIsIdentifierAndType() {
    EntityCacheKey key =
        EntityCacheKey.of(NameIdentifier.of("m1", "c1"), Entity.EntityType.CATALOG);
    Assertions.assertEquals("m1.c1:CATALOG", RedisKeyspace.member(key));
  }

  @Test
  void testFencePathsCoverEveryAncestor() {
    Assertions.assertEquals(
        ImmutableList.of("m1"), RedisKeyspace.fencePaths(NameIdentifier.of("m1"), SEP));
    Assertions.assertEquals(
        ImmutableList.of("m1", "m1.c1", "m1.c1.s1", "m1.c1.s1.t1"),
        RedisKeyspace.fencePaths(NameIdentifier.of("m1", "c1", "s1", "t1"), SEP));
  }

  @Test
  void testFencePathsCoverNestedSchemaPrefixes() {
    Assertions.assertEquals(
        ImmutableList.of("m1", "m1.c1", "m1.c1.raw", "m1.c1.raw:events", "m1.c1.raw:events:2024"),
        RedisKeyspace.fencePaths(NameIdentifier.of("m1", "c1", "raw:events:2024"), SEP));
    Assertions.assertEquals(
        ImmutableList.of("m1", "m1.c1", "m1.c1.raw", "m1.c1.raw:events", "m1.c1.raw:events.t1"),
        RedisKeyspace.fencePaths(NameIdentifier.of("m1", "c1", "raw:events", "t1"), SEP));
  }

  @Test
  void testFencePathsIgnoreSeparatorOutsideTheSchemaLevel() {
    // Only the schema level can carry nested levels; a separator elsewhere is a plain character.
    Assertions.assertEquals(
        ImmutableList.of("m1", "m1.c1", "m1.c1.s1", "m1.c1.s1.a:b"),
        RedisKeyspace.fencePaths(NameIdentifier.of("m1", "c1", "s1", "a:b"), SEP));
    Assertions.assertEquals(
        ImmutableList.of("m1", "m1.c1", "m1.c1.a:b"),
        RedisKeyspace.fencePaths(NameIdentifier.of("m1", "c1", "a:b"), ""));
  }

  @Test
  void testDescendantPrefixesUseTheLevelBoundary() {
    Assertions.assertEquals(
        ImmutableList.of("m1.c1."),
        RedisKeyspace.descendantPrefixes(
            EntityCacheKey.of(NameIdentifier.of("m1", "c1"), Entity.EntityType.CATALOG), SEP));
    Assertions.assertEquals(
        ImmutableList.of("m1.c1.s1.", "m1.c1.s1:"),
        RedisKeyspace.descendantPrefixes(
            EntityCacheKey.of(NameIdentifier.of("m1", "c1", "s1"), Entity.EntityType.SCHEMA), SEP));
    Assertions.assertEquals(
        ImmutableList.of("m1.c1.s1."),
        RedisKeyspace.descendantPrefixes(
            EntityCacheKey.of(NameIdentifier.of("m1", "c1", "s1"), Entity.EntityType.SCHEMA), ""));
  }

  @Test
  void testScanPatternsAndFenceDetection() {
    Assertions.assertEquals("ns:*", keyspace.allKeysPattern());
    Assertions.assertEquals("ns:{*}:IDX", keyspace.allIndexKeysPattern());
    Assertions.assertTrue(RedisKeyspace.isFenceKey("ns:{m1}:F:m1.c1"));
    Assertions.assertFalse(RedisKeyspace.isFenceKey("ns:{m1}:D:m1.c1:CATALOG"));
    Assertions.assertFalse(RedisKeyspace.isFenceKey("ns:{m1}:IDX"));
  }

  @Test
  void testNamespaceMustNotContainHashTagBraces() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> new RedisKeyspace("a{b}"));
    Assertions.assertThrows(IllegalArgumentException.class, () -> new RedisKeyspace(" "));
  }
}
