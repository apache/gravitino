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
import java.util.List;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;

public class TestCacheIndex {
  private RadixTree<StoreEntityCacheKey> indexTree;

  private NameIdentifier ident1;
  private NameIdentifier ident2;
  private NameIdentifier ident3;
  private NameIdentifier ident4;
  private NameIdentifier ident5;
  private NameIdentifier ident6;

  private StoreEntityCacheKey key1;
  private StoreEntityCacheKey key2;
  private StoreEntityCacheKey key3;
  private StoreEntityCacheKey key4;
  private StoreEntityCacheKey key5;
  private StoreEntityCacheKey key6;

  @BeforeEach
  void setUp() {
    indexTree = new ConcurrentRadixTree<>(new DefaultCharArrayNodeFactory());
    ident1 = NameIdentifier.of("metalake1", "catalog1", "schema1");
    ident2 = NameIdentifier.of("metalake2", "catalog2", "schema2");
    ident3 = NameIdentifier.of("metalake1", "catalog1", "schema1", "table");
    ident4 = NameIdentifier.of("metalake1", "catalog1", "schema1", "topic");
    ident5 = NameIdentifier.of("metalake1", "catalog2", "schema1", "table");
    ident6 = NameIdentifier.of("metalake1", "catalog1", "schema2", "table");

    key1 = StoreEntityCacheKey.of(ident1, Entity.EntityType.SCHEMA);
    key2 = StoreEntityCacheKey.of(ident2, Entity.EntityType.SCHEMA);
    key3 = StoreEntityCacheKey.of(ident3, Entity.EntityType.TABLE);
    key4 = StoreEntityCacheKey.of(ident4, Entity.EntityType.TOPIC);
    key5 = StoreEntityCacheKey.of(ident5, Entity.EntityType.TABLE);
    key6 = StoreEntityCacheKey.of(ident6, Entity.EntityType.TABLE);

    addIndex(indexTree, key6);
    addIndex(indexTree, key5);
    addIndex(indexTree, key4);
    addIndex(indexTree, key3);
    addIndex(indexTree, key2);
    addIndex(indexTree, key1);
  }

  @Test
  void testAddIndex() {
    Assertions.assertEquals(6, indexTree.size());
  }

  @Test
  void testGetFromByMetalakePrefix() {
    List<StoreEntityCacheKey> storeEntityCacheKeys =
        ImmutableList.copyOf(indexTree.getValuesForKeysStartingWith("metalake1"));

    Assertions.assertEquals(5, storeEntityCacheKeys.size());
    Assertions.assertTrue(storeEntityCacheKeys.contains(key1));
    Assertions.assertTrue(storeEntityCacheKeys.contains(key3));
    Assertions.assertTrue(storeEntityCacheKeys.contains(key4));
    Assertions.assertTrue(storeEntityCacheKeys.contains(key5));
    Assertions.assertTrue(storeEntityCacheKeys.contains(key6));
  }

  @Test
  void testGetByCatalogPrefix() {
    List<StoreEntityCacheKey> storeEntityCacheKeys =
        ImmutableList.copyOf(indexTree.getValuesForKeysStartingWith("metalake1.catalog1"));

    Assertions.assertEquals(4, storeEntityCacheKeys.size());
    Assertions.assertTrue(storeEntityCacheKeys.contains(key1));
    Assertions.assertTrue(storeEntityCacheKeys.contains(key3));
    Assertions.assertTrue(storeEntityCacheKeys.contains(key4));
    Assertions.assertTrue(storeEntityCacheKeys.contains(key6));

    storeEntityCacheKeys =
        ImmutableList.copyOf(indexTree.getValuesForKeysStartingWith("metalake1.catalog2"));
    Assertions.assertEquals(1, storeEntityCacheKeys.size());
    Assertions.assertTrue(storeEntityCacheKeys.contains(key5));
  }

  @Test
  void testGetBySchemaPrefix() {
    List<StoreEntityCacheKey> storeEntityCacheKeys =
        ImmutableList.copyOf(indexTree.getValuesForKeysStartingWith("metalake1.catalog1.schema1"));

    Assertions.assertEquals(3, storeEntityCacheKeys.size());
    Assertions.assertTrue(storeEntityCacheKeys.contains(key1));
    Assertions.assertTrue(storeEntityCacheKeys.contains(key3));
    Assertions.assertTrue(storeEntityCacheKeys.contains(key4));

    storeEntityCacheKeys =
        ImmutableList.copyOf(indexTree.getValuesForKeysStartingWith("metalake1.catalog1.schema2"));
    Assertions.assertEquals(1, storeEntityCacheKeys.size());
    Assertions.assertTrue(storeEntityCacheKeys.contains(key6));

    storeEntityCacheKeys =
        ImmutableList.copyOf(indexTree.getValuesForKeysStartingWith("metalake1.catalog2.schema1"));
    Assertions.assertEquals(1, storeEntityCacheKeys.size());
    Assertions.assertTrue(storeEntityCacheKeys.contains(key5));
  }

  @Test
  void testGetByExactKey() {
    StoreEntityCacheKey storeEntityCacheKey = indexTree.getValueForExactKey(key1.toString());
    Assertions.assertEquals(key1, storeEntityCacheKey);

    storeEntityCacheKey = indexTree.getValueForExactKey(key2.toString());
    Assertions.assertEquals(key2, storeEntityCacheKey);

    storeEntityCacheKey = indexTree.getValueForExactKey(key3.toString());
    Assertions.assertEquals(key3, storeEntityCacheKey);

    storeEntityCacheKey = indexTree.getValueForExactKey(key4.toString());
    Assertions.assertEquals(key4, storeEntityCacheKey);

    storeEntityCacheKey = indexTree.getValueForExactKey(key5.toString());
    Assertions.assertEquals(key5, storeEntityCacheKey);

    storeEntityCacheKey = indexTree.getValueForExactKey(key6.toString());
    Assertions.assertEquals(key6, storeEntityCacheKey);
  }

  private void addIndex(
      RadixTree<StoreEntityCacheKey> indexTree, NameIdentifier ident, Entity entity) {
    StoreEntityCacheKey storeEntityCacheKey = StoreEntityCacheKey.of(ident, entity.type());
    indexTree.put(storeEntityCacheKey.toString(), storeEntityCacheKey);
  }

  private void addIndex(
      RadixTree<StoreEntityCacheKey> indexTree, StoreEntityCacheKey storeEntityCacheKey) {
    indexTree.put(storeEntityCacheKey.toString(), storeEntityCacheKey);
  }
}
