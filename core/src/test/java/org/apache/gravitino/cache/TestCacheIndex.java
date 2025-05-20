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
  private RadixTree<EntityCacheKey> indexTree;

  private NameIdentifier ident1;
  private NameIdentifier ident2;
  private NameIdentifier ident3;
  private NameIdentifier ident4;
  private NameIdentifier ident5;
  private NameIdentifier ident6;

  private EntityCacheKey key1;
  private EntityCacheKey key2;
  private EntityCacheKey key3;
  private EntityCacheKey key4;
  private EntityCacheKey key5;
  private EntityCacheKey key6;

  @BeforeEach
  void setUp() {
    indexTree = new ConcurrentRadixTree<>(new DefaultCharArrayNodeFactory());
    ident1 = NameIdentifier.of("metalake1", "catalog1", "schema1");
    ident2 = NameIdentifier.of("metalake2", "catalog2", "schema2");
    ident3 = NameIdentifier.of("metalake1", "catalog1", "schema1", "table");
    ident4 = NameIdentifier.of("metalake1", "catalog1", "schema1", "topic");
    ident5 = NameIdentifier.of("metalake1", "catalog2", "schema1", "table");
    ident6 = NameIdentifier.of("metalake1", "catalog1", "schema2", "table");

    key1 = EntityCacheKey.of(ident1, Entity.EntityType.SCHEMA);
    key2 = EntityCacheKey.of(ident2, Entity.EntityType.SCHEMA);
    key3 = EntityCacheKey.of(ident3, Entity.EntityType.TABLE);
    key4 = EntityCacheKey.of(ident4, Entity.EntityType.TOPIC);
    key5 = EntityCacheKey.of(ident5, Entity.EntityType.TABLE);
    key6 = EntityCacheKey.of(ident6, Entity.EntityType.TABLE);

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
    List<EntityCacheKey> entityCacheKeys =
        ImmutableList.copyOf(indexTree.getValuesForKeysStartingWith("metalake1"));

    Assertions.assertEquals(5, entityCacheKeys.size());
    Assertions.assertTrue(entityCacheKeys.contains(key1));
    Assertions.assertTrue(entityCacheKeys.contains(key3));
    Assertions.assertTrue(entityCacheKeys.contains(key4));
    Assertions.assertTrue(entityCacheKeys.contains(key5));
    Assertions.assertTrue(entityCacheKeys.contains(key6));
  }

  @Test
  void testGetByCatalogPrefix() {
    List<EntityCacheKey> entityCacheKeys =
        ImmutableList.copyOf(indexTree.getValuesForKeysStartingWith("metalake1.catalog1"));

    Assertions.assertEquals(4, entityCacheKeys.size());
    Assertions.assertTrue(entityCacheKeys.contains(key1));
    Assertions.assertTrue(entityCacheKeys.contains(key3));
    Assertions.assertTrue(entityCacheKeys.contains(key4));
    Assertions.assertTrue(entityCacheKeys.contains(key6));

    entityCacheKeys =
        ImmutableList.copyOf(indexTree.getValuesForKeysStartingWith("metalake1.catalog2"));
    Assertions.assertEquals(1, entityCacheKeys.size());
    Assertions.assertTrue(entityCacheKeys.contains(key5));
  }

  @Test
  void testGetBySchemaPrefix() {
    List<EntityCacheKey> entityCacheKeys =
        ImmutableList.copyOf(indexTree.getValuesForKeysStartingWith("metalake1.catalog1.schema1"));

    Assertions.assertEquals(3, entityCacheKeys.size());
    Assertions.assertTrue(entityCacheKeys.contains(key1));
    Assertions.assertTrue(entityCacheKeys.contains(key3));
    Assertions.assertTrue(entityCacheKeys.contains(key4));

    entityCacheKeys =
        ImmutableList.copyOf(indexTree.getValuesForKeysStartingWith("metalake1.catalog1.schema2"));
    Assertions.assertEquals(1, entityCacheKeys.size());
    Assertions.assertTrue(entityCacheKeys.contains(key6));

    entityCacheKeys =
        ImmutableList.copyOf(indexTree.getValuesForKeysStartingWith("metalake1.catalog2.schema1"));
    Assertions.assertEquals(1, entityCacheKeys.size());
    Assertions.assertTrue(entityCacheKeys.contains(key5));
  }

  @Test
  void testGetByExactKey() {
    EntityCacheKey entityCacheKey = indexTree.getValueForExactKey(key1.toString());
    Assertions.assertEquals(key1, entityCacheKey);

    entityCacheKey = indexTree.getValueForExactKey(key2.toString());
    Assertions.assertEquals(key2, entityCacheKey);

    entityCacheKey = indexTree.getValueForExactKey(key3.toString());
    Assertions.assertEquals(key3, entityCacheKey);

    entityCacheKey = indexTree.getValueForExactKey(key4.toString());
    Assertions.assertEquals(key4, entityCacheKey);

    entityCacheKey = indexTree.getValueForExactKey(key5.toString());
    Assertions.assertEquals(key5, entityCacheKey);

    entityCacheKey = indexTree.getValueForExactKey(key6.toString());
    Assertions.assertEquals(key6, entityCacheKey);
  }

  private void addIndex(RadixTree<EntityCacheKey> indexTree, NameIdentifier ident, Entity entity) {
    EntityCacheKey entityCacheKey = EntityCacheKey.of(ident, entity.type());
    indexTree.put(entityCacheKey.toString(), entityCacheKey);
  }

  private void addIndex(RadixTree<EntityCacheKey> indexTree, EntityCacheKey entityCacheKey) {
    indexTree.put(entityCacheKey.toString(), entityCacheKey);
  }
}
