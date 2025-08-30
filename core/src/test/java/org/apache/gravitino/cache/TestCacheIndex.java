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
  private RadixTree<EntityCacheRelationKey> indexTree;

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

  private EntityCacheRelationKey key1;
  private EntityCacheRelationKey key2;
  private EntityCacheRelationKey key3;
  private EntityCacheRelationKey key4;
  private EntityCacheRelationKey key5;
  private EntityCacheRelationKey key6;
  private EntityCacheRelationKey key7;
  private EntityCacheRelationKey key8;
  private EntityCacheRelationKey key9;
  private EntityCacheRelationKey key10;
  private EntityCacheRelationKey key11;
  private EntityCacheRelationKey key12;

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

    key1 = EntityCacheRelationKey.of(ident1, Entity.EntityType.SCHEMA);
    key2 = EntityCacheRelationKey.of(ident2, Entity.EntityType.SCHEMA);
    key3 = EntityCacheRelationKey.of(ident3, Entity.EntityType.TABLE);
    key4 = EntityCacheRelationKey.of(ident4, Entity.EntityType.TOPIC);
    key5 = EntityCacheRelationKey.of(ident5, Entity.EntityType.TABLE);
    key6 = EntityCacheRelationKey.of(ident6, Entity.EntityType.TABLE);

    key7 =
        EntityCacheRelationKey.of(
            ident7, Entity.EntityType.ROLE, SupportsRelationOperations.Type.ROLE_GROUP_REL);
    key8 =
        EntityCacheRelationKey.of(
            ident8, Entity.EntityType.ROLE, SupportsRelationOperations.Type.ROLE_USER_REL);

    key9 = EntityCacheRelationKey.of(ident9, Entity.EntityType.GROUP);
    key10 = EntityCacheRelationKey.of(ident10, Entity.EntityType.GROUP);
    key11 = EntityCacheRelationKey.of(ident11, Entity.EntityType.USER);
    key12 = EntityCacheRelationKey.of(ident12, Entity.EntityType.USER);

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
    List<EntityCacheRelationKey> storeEntityCacheRelationKeys =
        ImmutableList.copyOf(indexTree.getValuesForKeysStartingWith("metalake1"));

    Assertions.assertEquals(8, storeEntityCacheRelationKeys.size());
    Assertions.assertTrue(storeEntityCacheRelationKeys.contains(key1));
    Assertions.assertTrue(storeEntityCacheRelationKeys.contains(key3));
    Assertions.assertTrue(storeEntityCacheRelationKeys.contains(key4));
    Assertions.assertTrue(storeEntityCacheRelationKeys.contains(key5));
    Assertions.assertTrue(storeEntityCacheRelationKeys.contains(key6));
    Assertions.assertTrue(storeEntityCacheRelationKeys.contains(key7));
    Assertions.assertTrue(storeEntityCacheRelationKeys.contains(key9));
    Assertions.assertTrue(storeEntityCacheRelationKeys.contains(key10));

    List<EntityCacheRelationKey> storeEntityCacheRelationKeys2 =
        ImmutableList.copyOf(indexTree.getValuesForKeysStartingWith("metalake2"));

    Assertions.assertEquals(4, storeEntityCacheRelationKeys2.size());
    Assertions.assertTrue(storeEntityCacheRelationKeys2.contains(key2));
    Assertions.assertTrue(storeEntityCacheRelationKeys2.contains(key8));
    Assertions.assertTrue(storeEntityCacheRelationKeys2.contains(key11));
    Assertions.assertTrue(storeEntityCacheRelationKeys2.contains(key12));
  }

  @Test
  void testGetByCatalogPrefix() {
    List<EntityCacheRelationKey> storeEntityCacheRelationKeys =
        ImmutableList.copyOf(indexTree.getValuesForKeysStartingWith("metalake1.catalog1"));

    Assertions.assertEquals(4, storeEntityCacheRelationKeys.size());
    Assertions.assertTrue(storeEntityCacheRelationKeys.contains(key1));
    Assertions.assertTrue(storeEntityCacheRelationKeys.contains(key3));
    Assertions.assertTrue(storeEntityCacheRelationKeys.contains(key4));
    Assertions.assertTrue(storeEntityCacheRelationKeys.contains(key6));

    storeEntityCacheRelationKeys =
        ImmutableList.copyOf(indexTree.getValuesForKeysStartingWith("metalake1.catalog2"));
    Assertions.assertEquals(1, storeEntityCacheRelationKeys.size());
    Assertions.assertTrue(storeEntityCacheRelationKeys.contains(key5));
  }

  @Test
  void testGetBySchemaPrefix() {
    List<EntityCacheRelationKey> storeEntityCacheRelationKeys =
        ImmutableList.copyOf(indexTree.getValuesForKeysStartingWith("metalake1.catalog1.schema1"));

    Assertions.assertEquals(3, storeEntityCacheRelationKeys.size());
    Assertions.assertTrue(storeEntityCacheRelationKeys.contains(key1));
    Assertions.assertTrue(storeEntityCacheRelationKeys.contains(key3));
    Assertions.assertTrue(storeEntityCacheRelationKeys.contains(key4));

    storeEntityCacheRelationKeys =
        ImmutableList.copyOf(indexTree.getValuesForKeysStartingWith("metalake1.catalog1.schema2"));
    Assertions.assertEquals(1, storeEntityCacheRelationKeys.size());
    Assertions.assertTrue(storeEntityCacheRelationKeys.contains(key6));

    storeEntityCacheRelationKeys =
        ImmutableList.copyOf(indexTree.getValuesForKeysStartingWith("metalake1.catalog2.schema1"));
    Assertions.assertEquals(1, storeEntityCacheRelationKeys.size());
    Assertions.assertTrue(storeEntityCacheRelationKeys.contains(key5));
  }

  @Test
  void testGetByExactKey() {
    EntityCacheRelationKey storeEntityCacheRelationKey =
        indexTree.getValueForExactKey(key1.toString());
    Assertions.assertEquals(key1, storeEntityCacheRelationKey);

    storeEntityCacheRelationKey = indexTree.getValueForExactKey(key2.toString());
    Assertions.assertEquals(key2, storeEntityCacheRelationKey);

    storeEntityCacheRelationKey = indexTree.getValueForExactKey(key3.toString());
    Assertions.assertEquals(key3, storeEntityCacheRelationKey);

    storeEntityCacheRelationKey = indexTree.getValueForExactKey(key4.toString());
    Assertions.assertEquals(key4, storeEntityCacheRelationKey);

    storeEntityCacheRelationKey = indexTree.getValueForExactKey(key5.toString());
    Assertions.assertEquals(key5, storeEntityCacheRelationKey);

    storeEntityCacheRelationKey = indexTree.getValueForExactKey(key6.toString());
    Assertions.assertEquals(key6, storeEntityCacheRelationKey);

    storeEntityCacheRelationKey = indexTree.getValueForExactKey(key7.toString());
    Assertions.assertEquals(key7, storeEntityCacheRelationKey);

    storeEntityCacheRelationKey = indexTree.getValueForExactKey(key8.toString());
    Assertions.assertEquals(key8, storeEntityCacheRelationKey);

    storeEntityCacheRelationKey = indexTree.getValueForExactKey(key9.toString());
    Assertions.assertEquals(key9, storeEntityCacheRelationKey);

    storeEntityCacheRelationKey = indexTree.getValueForExactKey(key10.toString());
    Assertions.assertEquals(key10, storeEntityCacheRelationKey);

    storeEntityCacheRelationKey = indexTree.getValueForExactKey(key11.toString());
    Assertions.assertEquals(key11, storeEntityCacheRelationKey);

    storeEntityCacheRelationKey = indexTree.getValueForExactKey(key12.toString());
    Assertions.assertEquals(key12, storeEntityCacheRelationKey);
  }

  private void addIndex(
      RadixTree<EntityCacheRelationKey> indexTree,
      EntityCacheRelationKey storeEntityCacheRelationKey) {
    indexTree.put(storeEntityCacheRelationKey.toString(), storeEntityCacheRelationKey);
  }
}
