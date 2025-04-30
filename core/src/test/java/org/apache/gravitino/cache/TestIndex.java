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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.googlecode.concurrenttrees.radix.ConcurrentRadixTree;
import com.googlecode.concurrenttrees.radix.RadixTree;
import com.googlecode.concurrenttrees.radix.node.concrete.DefaultCharSequenceNodeFactory;
import java.util.List;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestIndex {
  private RadixTree<NameIdentifier> indexTree;
  private NameIdentifier ident1;
  private NameIdentifier ident2;
  private NameIdentifier ident3;
  private NameIdentifier ident4;
  private NameIdentifier ident5;
  private NameIdentifier ident6;

  @BeforeEach
  void setUp() {
    indexTree = new ConcurrentRadixTree<>(new DefaultCharSequenceNodeFactory());
    ident1 = NameIdentifier.of("metalake1", "catalog1", "schema1");
    ident2 = NameIdentifier.of("metalake2", "catalog2", "schema2");
    ident3 = NameIdentifier.of("metalake1", "catalog1", "schema1", "user");
    ident4 = NameIdentifier.of("metalake1", "catalog1", "schema1", "order");
    ident5 = NameIdentifier.of("metalake1", "catalog2", "schema1", "topic");
    ident6 = NameIdentifier.of("metalake1", "catalog1", "schema2", "table");

    addIndex(ident6);
    addIndex(ident5);
    addIndex(ident4);
    addIndex(ident3);
    addIndex(ident2);
    addIndex(ident1);
  }

  @Test
  void testAddIndex() {
    Assertions.assertEquals(6, indexTree.size());
  }

  @Test
  void testCaffeineCache() {
    Cache<NameIdentifier, Entity> byName =
        Caffeine.newBuilder()
            .executor(Runnable::run)
            .removalListener(
                (NameIdentifier identifier, Entity entity, RemovalCause cause) -> {
                  if (cause == RemovalCause.EXPIRED) {
                    System.out.println("Cache entry expired: " + identifier);
                  }
                })
            .build();

    byName.put(ident1, TestUtil.getTestSchemaEntity());
    byName.put(ident2, TestUtil.getTestSchemaEntity());
  }

  @Test
  void testGetFromMetalakeByPrefix() {
    Iterable<NameIdentifier> values = indexTree.getValuesForKeysStartingWith("metalake1");
    List<NameIdentifier> identList = TestUtil.toList(values);

    Assertions.assertEquals(5, identList.size());
    Assertions.assertTrue(identList.contains(ident1));
    Assertions.assertTrue(identList.contains(ident3));
    Assertions.assertTrue(identList.contains(ident4));
    Assertions.assertTrue(identList.contains(ident5));
    Assertions.assertTrue(identList.contains(ident6));
  }

  @Test
  void testGetFromCatalogByPrefix() {
    Iterable<NameIdentifier> values = indexTree.getValuesForKeysStartingWith("metalake1");
    List<NameIdentifier> identList = TestUtil.toList(values);

    Assertions.assertEquals(5, identList.size());
    Assertions.assertTrue(identList.contains(ident1));
    Assertions.assertTrue(identList.contains(ident3));
    Assertions.assertTrue(identList.contains(ident4));
    Assertions.assertTrue(identList.contains(ident5));
    Assertions.assertTrue(identList.contains(ident6));

    values = indexTree.getValuesForKeysStartingWith("metalake1.catalog2");
    identList = TestUtil.toList(values);

    Assertions.assertEquals(1, identList.size());
    Assertions.assertTrue(identList.contains(ident5));
  }

  @Test
  void testGetFromSchemaByPrefix() {
    String prefix = "metalake1.catalog1.schema1";
    Iterable<NameIdentifier> values = indexTree.getValuesForKeysStartingWith(prefix);
    List<NameIdentifier> identList = TestUtil.toList(values);

    Assertions.assertEquals(3, identList.size());
    Assertions.assertTrue(identList.contains(ident1));
    Assertions.assertTrue(identList.contains(ident3));
    Assertions.assertTrue(identList.contains(ident4));

    prefix = "metalake1.catalog1.schema2";
    values = indexTree.getValuesForKeysStartingWith(prefix);
    identList = TestUtil.toList(values);

    Assertions.assertEquals(1, identList.size());
    Assertions.assertTrue(identList.contains(ident6));

    prefix = "metalake1.catalog2.schema1";
    values = indexTree.getValuesForKeysStartingWith(prefix);
    identList = TestUtil.toList(values);

    Assertions.assertEquals(1, identList.size());
    Assertions.assertTrue(identList.contains(ident5));
  }

  @Test
  void testDeleteIndexUseMetalake() {
    String prefix = "metalake1";
    indexTree.getKeysStartingWith(prefix).forEach(node -> indexTree.remove(node));

    Assertions.assertEquals(1, indexTree.size());
    Assertions.assertNotNull(indexTree.getValueForExactKey(ident2.toString()));
  }

  @Test
  void testDeleteIndexUseCatalog() {
    String prefix = "metalake1.catalog1";
    indexTree.getKeysStartingWith(prefix).forEach(node -> indexTree.remove(node));

    Assertions.assertEquals(2, indexTree.size());
    Assertions.assertNotNull(indexTree.getValueForExactKey(ident2.toString()));
    Assertions.assertNotNull(indexTree.getValueForExactKey(ident5.toString()));
  }

  @Test
  void testDeleteIndexUseSchema() {
    String prefix = "metalake1.catalog1.schema1.user";
    indexTree.getKeysStartingWith(prefix).forEach(node -> indexTree.remove(node));

    Assertions.assertEquals(5, indexTree.size());
    Assertions.assertNotNull(indexTree.getValueForExactKey(ident1.toString()));
    Assertions.assertNotNull(indexTree.getValueForExactKey(ident2.toString()));
    Assertions.assertNotNull(indexTree.getValueForExactKey(ident4.toString()));
    Assertions.assertNotNull(indexTree.getValueForExactKey(ident5.toString()));
    Assertions.assertNotNull(indexTree.getValueForExactKey(ident6.toString()));
  }

  @Test
  void testDeleteIndexUseTable() {
    String prefix = "metalake1.catalog1.schema2.table";
    indexTree.getKeysStartingWith(prefix).forEach(node -> indexTree.remove(node));

    Assertions.assertEquals(5, indexTree.size());
    Assertions.assertNotNull(indexTree.getValueForExactKey(ident1.toString()));
    Assertions.assertNotNull(indexTree.getValueForExactKey(ident2.toString()));
  }

  private void addIndex(NameIdentifier ident) {
    indexTree.put(ident.toString(), ident);
  }
}
