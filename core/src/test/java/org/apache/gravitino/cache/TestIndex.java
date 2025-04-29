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

import java.util.SortedMap;
import org.apache.commons.collections4.Trie;
import org.apache.commons.collections4.trie.PatriciaTrie;
import org.apache.gravitino.NameIdentifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestIndex {
  private Trie<String, NameIdentifier> indexTree;
  private NameIdentifier ident1;
  private NameIdentifier ident2;
  private NameIdentifier ident3;
  private NameIdentifier ident4;
  private NameIdentifier ident5;
  private NameIdentifier ident6;

  @BeforeEach
  void setUp() {
    indexTree = new PatriciaTrie();
    ident1 = NameIdentifier.of("metalake1", "catalog1", "schema1");
    ident2 = NameIdentifier.of("metalake2", "catalog2", "schema2");
    ident3 = NameIdentifier.of("metalake1", "catalog1", "schema1", "table");
    ident4 = NameIdentifier.of("metalake1", "catalog1", "schema1", "topic");
    ident5 = NameIdentifier.of("metalake1", "catalog2", "schema1", "table");
    ident6 = NameIdentifier.of("metalake1", "catalog1", "schema2", "table");

    addIndex(indexTree, ident6);
    addIndex(indexTree, ident5);
    addIndex(indexTree, ident4);
    addIndex(indexTree, ident3);
    addIndex(indexTree, ident2);
    addIndex(indexTree, ident1);
  }

  @Test
  void testAddIndex() {
    Assertions.assertEquals(6, indexTree.size());
  }

  @Test
  void testGetFromMetalakeByPrefix() {
    SortedMap<String, NameIdentifier> prefixMap = indexTree.prefixMap("metalake1");

    Assertions.assertEquals(5, prefixMap.size());
    Assertions.assertEquals(ident1, prefixMap.get(ident1.toString()));
    Assertions.assertEquals(ident3, prefixMap.get(ident3.toString()));
    Assertions.assertEquals(ident4, prefixMap.get(ident4.toString()));
    Assertions.assertEquals(ident5, prefixMap.get(ident5.toString()));
    Assertions.assertEquals(ident6, prefixMap.get(ident6.toString()));
  }

  @Test
  void testGetFromCatalogByPrefix() {
    SortedMap<String, NameIdentifier> prefixMap = indexTree.prefixMap("metalake1.catalog1");

    Assertions.assertEquals(4, prefixMap.size());
    Assertions.assertEquals(ident1, prefixMap.get(ident1.toString()));
    Assertions.assertEquals(ident3, prefixMap.get(ident3.toString()));
    Assertions.assertEquals(ident4, prefixMap.get(ident4.toString()));
    Assertions.assertEquals(ident6, prefixMap.get(ident6.toString()));

    prefixMap = indexTree.prefixMap("metalake1.catalog2");
    Assertions.assertEquals(1, prefixMap.size());
    Assertions.assertEquals(ident5, prefixMap.get(ident5.toString()));
  }

  @Test
  void testGetFromSchemaByPrefix() {
    SortedMap<String, NameIdentifier> prefixMap = indexTree.prefixMap("metalake1.catalog1.schema1");

    Assertions.assertEquals(3, prefixMap.size());
    Assertions.assertEquals(ident1, prefixMap.get(ident1.toString()));
    Assertions.assertEquals(ident3, prefixMap.get(ident3.toString()));
    Assertions.assertEquals(ident4, prefixMap.get(ident4.toString()));

    prefixMap = indexTree.prefixMap("metalake2.catalog2.schema2");
    Assertions.assertEquals(1, prefixMap.size());
    Assertions.assertEquals(ident2, prefixMap.get(ident2.toString()));

    prefixMap = indexTree.prefixMap("metalake1.catalog2.schema1");
    Assertions.assertEquals(1, prefixMap.size());
    Assertions.assertEquals(ident5, prefixMap.get(ident5.toString()));
  }

  private void addIndex(Trie<String, NameIdentifier> indexTree, NameIdentifier ident) {
    indexTree.put(ident.toString(), ident);
  }
}
