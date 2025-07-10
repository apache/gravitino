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
import com.googlecode.concurrenttrees.radix.ConcurrentRadixTree;
import com.googlecode.concurrenttrees.radix.RadixTree;
import com.googlecode.concurrenttrees.radix.node.concrete.DefaultCharArrayNodeFactory;
import java.util.List;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestCacheIndex {
  private RadixTree<EntityCacheKey> indexTree;

  private NameIdentifier ident1;
  private NameIdentifier ident2;
  private NameIdentifier ident3;
  private NameIdentifier ident4;
  private NameIdentifier ident5;
  private NameIdentifier ident6;
  private NameIdentifier ident7;
  private NameIdentifier ident8;
  private NameIdentifier ident9;
  private NameIdentifier ident10;
  private NameIdentifier ident11;
  private NameIdentifier ident12;

  private EntityCacheKey key1;
  private EntityCacheKey key2;
  private EntityCacheKey key3;
  private EntityCacheKey key4;
  private EntityCacheKey key5;
  private EntityCacheKey key6;
  private EntityCacheKey key7;
  private EntityCacheKey key8;
  private EntityCacheKey key9;
  private EntityCacheKey key10;
  private EntityCacheKey key11;
  private EntityCacheKey key12;

  @BeforeEach
  void setUp() {
    indexTree = new ConcurrentRadixTree<>(new DefaultCharArrayNodeFactory());
    ident1 = NameIdentifier.of("metalake1", "catalog1", "schema1");
    ident2 = NameIdentifier.of("metalake2", "catalog2", "schema2");
    ident3 = NameIdentifier.of("metalake1", "catalog1", "schema1", "table");
    ident4 = NameIdentifier.of("metalake1", "catalog1", "schema1", "topic");
    ident5 = NameIdentifier.of("metalake1", "catalog2", "schema1", "table");
    ident6 = NameIdentifier.of("metalake1", "catalog1", "schema2", "table");

    ident7 = NameIdentifierUtil.ofRole("metalake1", "role1");
    ident8 = NameIdentifierUtil.ofRole("metalake2", "role2");

    ident9 = NameIdentifierUtil.ofGroup("metalake1", "group1");
    ident10 = NameIdentifierUtil.ofGroup("metalake1", "group2");

    ident11 = NameIdentifierUtil.ofUser("metalake2", "user1");
    ident12 = NameIdentifierUtil.ofUser("metalake2", "user2");

    key1 = EntityCacheKey.of(ident1, Entity.EntityType.SCHEMA);
    key2 = EntityCacheKey.of(ident2, Entity.EntityType.SCHEMA);
    key3 = EntityCacheKey.of(ident3, Entity.EntityType.TABLE);
    key4 = EntityCacheKey.of(ident4, Entity.EntityType.TOPIC);
    key5 = EntityCacheKey.of(ident5, Entity.EntityType.TABLE);
    key6 = EntityCacheKey.of(ident6, Entity.EntityType.TABLE);

    key7 =
        EntityCacheKey.of(
            ident7, Entity.EntityType.ROLE, SupportsRelationOperations.Type.ROLE_GROUP_REL);
    key8 =
        EntityCacheKey.of(
            ident8, Entity.EntityType.ROLE, SupportsRelationOperations.Type.ROLE_USER_REL);

    key9 = EntityCacheKey.of(ident9, Entity.EntityType.GROUP);
    key10 = EntityCacheKey.of(ident10, Entity.EntityType.GROUP);
    key11 = EntityCacheKey.of(ident11, Entity.EntityType.USER);
    key12 = EntityCacheKey.of(ident12, Entity.EntityType.USER);

    addIndex(indexTree, key12);
    addIndex(indexTree, key11);
    addIndex(indexTree, key10);
    addIndex(indexTree, key9);
    addIndex(indexTree, key8);
    addIndex(indexTree, key7);
    addIndex(indexTree, key6);
    addIndex(indexTree, key5);
    addIndex(indexTree, key4);
    addIndex(indexTree, key3);
    addIndex(indexTree, key2);
    addIndex(indexTree, key1);
  }

  @Test
  void testAddIndex() {
    Assertions.assertEquals(12, indexTree.size());
  }

  @Test
  void testGetFromByMetalakePrefix() {
    List<EntityCacheKey> storeEntityCacheKeys =
        ImmutableList.copyOf(indexTree.getValuesForKeysStartingWith("metalake1"));

    Assertions.assertEquals(8, storeEntityCacheKeys.size());
    Assertions.assertTrue(storeEntityCacheKeys.contains(key1));
    Assertions.assertTrue(storeEntityCacheKeys.contains(key3));
    Assertions.assertTrue(storeEntityCacheKeys.contains(key4));
    Assertions.assertTrue(storeEntityCacheKeys.contains(key5));
    Assertions.assertTrue(storeEntityCacheKeys.contains(key6));
    Assertions.assertTrue(storeEntityCacheKeys.contains(key7));
    Assertions.assertTrue(storeEntityCacheKeys.contains(key9));
    Assertions.assertTrue(storeEntityCacheKeys.contains(key10));

    List<EntityCacheKey> storeEntityCacheKeys2 =
        ImmutableList.copyOf(indexTree.getValuesForKeysStartingWith("metalake2"));

    Assertions.assertEquals(4, storeEntityCacheKeys2.size());
    Assertions.assertTrue(storeEntityCacheKeys2.contains(key2));
    Assertions.assertTrue(storeEntityCacheKeys2.contains(key8));
    Assertions.assertTrue(storeEntityCacheKeys2.contains(key11));
    Assertions.assertTrue(storeEntityCacheKeys2.contains(key12));
  }

  @Test
  void testGetByCatalogPrefix() {
    List<EntityCacheKey> storeEntityCacheKeys =
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
    List<EntityCacheKey> storeEntityCacheKeys =
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
    EntityCacheKey storeEntityCacheKey = indexTree.getValueForExactKey(key1.toString());
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

    storeEntityCacheKey = indexTree.getValueForExactKey(key7.toString());
    Assertions.assertEquals(key7, storeEntityCacheKey);

    storeEntityCacheKey = indexTree.getValueForExactKey(key8.toString());
    Assertions.assertEquals(key8, storeEntityCacheKey);

    storeEntityCacheKey = indexTree.getValueForExactKey(key9.toString());
    Assertions.assertEquals(key9, storeEntityCacheKey);

    storeEntityCacheKey = indexTree.getValueForExactKey(key10.toString());
    Assertions.assertEquals(key10, storeEntityCacheKey);

    storeEntityCacheKey = indexTree.getValueForExactKey(key11.toString());
    Assertions.assertEquals(key11, storeEntityCacheKey);

    storeEntityCacheKey = indexTree.getValueForExactKey(key12.toString());
    Assertions.assertEquals(key12, storeEntityCacheKey);
  }

  private void addIndex(RadixTree<EntityCacheKey> indexTree, EntityCacheKey storeEntityCacheKey) {
    indexTree.put(storeEntityCacheKey.toString(), storeEntityCacheKey);
  }
}
