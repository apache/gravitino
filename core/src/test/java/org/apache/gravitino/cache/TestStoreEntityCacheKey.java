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

import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestStoreEntityCacheKey {

  @Test
  void testCreateStoreEntityCacheKeyUseConstructor() {
    NameIdentifier ident = NameIdentifier.of("m1.c1.s1.t1");
    StoreEntityCacheKey storeEntityCacheKey =
        new StoreEntityCacheKey(ident, Entity.EntityType.MODEL);
    Assertions.assertEquals(ident, storeEntityCacheKey.identifier());
    Assertions.assertEquals(Entity.EntityType.MODEL, storeEntityCacheKey.type());
  }

  @Test
  void testCreateStoreEntityCacheKeyUseStaticMethod() {
    NameIdentifier ident = NameIdentifier.of("m1.c1.s1.t1");
    StoreEntityCacheKey storeEntityCacheKey =
        StoreEntityCacheKey.of(ident, Entity.EntityType.MODEL);
    Assertions.assertEquals(ident, storeEntityCacheKey.identifier());
    Assertions.assertEquals(Entity.EntityType.MODEL, storeEntityCacheKey.type());
  }

  @Test
  void testCreateStoreEntityCacheKeyWithNullArguments() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> StoreEntityCacheKey.of(null, Entity.EntityType.MODEL));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> StoreEntityCacheKey.of(NameIdentifier.of("m1.c1.s1.t1"), null));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> StoreEntityCacheKey.of(null, null));
  }

  @Test
  void testOtherMethods() {
    NameIdentifier ident1 = NameIdentifier.of("m1.c1.s1.t1");
    NameIdentifier ident2 = NameIdentifier.of("m1.c1.s1.t2");
    NameIdentifier ident3 = NameIdentifier.of("m1.c1.s1.t1");

    StoreEntityCacheKey storeEntityCacheKey1 =
        StoreEntityCacheKey.of(ident1, Entity.EntityType.MODEL);
    StoreEntityCacheKey storeEntityCacheKey2 =
        StoreEntityCacheKey.of(ident2, Entity.EntityType.MODEL);
    StoreEntityCacheKey storeEntityCacheKey3 =
        StoreEntityCacheKey.of(ident1, Entity.EntityType.TABLE);
    StoreEntityCacheKey storeEntityCacheKey4 =
        StoreEntityCacheKey.of(ident2, Entity.EntityType.TABLE);
    StoreEntityCacheKey storeEntityCacheKey5 =
        StoreEntityCacheKey.of(ident3, Entity.EntityType.MODEL);

    Assertions.assertNotEquals(storeEntityCacheKey1, storeEntityCacheKey2);
    Assertions.assertNotEquals(storeEntityCacheKey1, storeEntityCacheKey3);
    Assertions.assertNotEquals(storeEntityCacheKey1, storeEntityCacheKey4);
    Assertions.assertNotEquals(storeEntityCacheKey2, storeEntityCacheKey3);
    Assertions.assertNotEquals(storeEntityCacheKey2, storeEntityCacheKey4);
    Assertions.assertNotEquals(storeEntityCacheKey3, storeEntityCacheKey4);
    Assertions.assertEquals(storeEntityCacheKey1, storeEntityCacheKey5);

    Assertions.assertNotEquals(storeEntityCacheKey1.hashCode(), storeEntityCacheKey2.hashCode());
    Assertions.assertNotEquals(storeEntityCacheKey1.hashCode(), storeEntityCacheKey3.hashCode());
    Assertions.assertNotEquals(storeEntityCacheKey1.hashCode(), storeEntityCacheKey4.hashCode());
    Assertions.assertNotEquals(storeEntityCacheKey2.hashCode(), storeEntityCacheKey3.hashCode());
    Assertions.assertNotEquals(storeEntityCacheKey2.hashCode(), storeEntityCacheKey4.hashCode());
    Assertions.assertNotEquals(storeEntityCacheKey3.hashCode(), storeEntityCacheKey4.hashCode());
    Assertions.assertEquals(storeEntityCacheKey1.hashCode(), storeEntityCacheKey5.hashCode());

    Assertions.assertEquals("m1.c1.s1.t1:mo", storeEntityCacheKey1.toString());
  }
}
