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

public class TestEntityCacheKey {

  @Test
  void testCreateMetaCacheKeyUseConstructor() {
    NameIdentifier ident = NameIdentifier.of("m1.c1.s1.t1");
    EntityCacheKey entityCacheKey = new EntityCacheKey(ident, Entity.EntityType.MODEL);
    Assertions.assertEquals(ident, entityCacheKey.identifier());
    Assertions.assertEquals(Entity.EntityType.MODEL, entityCacheKey.type());
  }

  @Test
  void testCreateMetaCacheKeyUseStaticMethod() {
    NameIdentifier ident = NameIdentifier.of("m1.c1.s1.t1");
    EntityCacheKey entityCacheKey = EntityCacheKey.of(ident, Entity.EntityType.MODEL);
    Assertions.assertEquals(ident, entityCacheKey.identifier());
    Assertions.assertEquals(Entity.EntityType.MODEL, entityCacheKey.type());
  }

  @Test
  void testCreateMetaCacheKeyWithNullArguments() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> EntityCacheKey.of(null, Entity.EntityType.MODEL));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> EntityCacheKey.of(NameIdentifier.of("m1.c1.s1.t1"), null));
    Assertions.assertThrows(IllegalArgumentException.class, () -> EntityCacheKey.of(null, null));
  }

  @Test
  void testOtherMethods() {
    NameIdentifier ident1 = NameIdentifier.of("m1.c1.s1.t1");
    NameIdentifier ident2 = NameIdentifier.of("m1.c1.s1.t2");
    NameIdentifier ident3 = NameIdentifier.of("m1.c1.s1.t1");

    EntityCacheKey entityCacheKey1 = EntityCacheKey.of(ident1, Entity.EntityType.MODEL);
    EntityCacheKey entityCacheKey2 = EntityCacheKey.of(ident2, Entity.EntityType.MODEL);
    EntityCacheKey entityCacheKey3 = EntityCacheKey.of(ident1, Entity.EntityType.TABLE);
    EntityCacheKey entityCacheKey4 = EntityCacheKey.of(ident2, Entity.EntityType.TABLE);
    EntityCacheKey entityCacheKey5 = EntityCacheKey.of(ident3, Entity.EntityType.MODEL);

    Assertions.assertNotEquals(entityCacheKey1, entityCacheKey2);
    Assertions.assertNotEquals(entityCacheKey1, entityCacheKey3);
    Assertions.assertNotEquals(entityCacheKey1, entityCacheKey4);
    Assertions.assertNotEquals(entityCacheKey2, entityCacheKey3);
    Assertions.assertNotEquals(entityCacheKey2, entityCacheKey4);
    Assertions.assertNotEquals(entityCacheKey3, entityCacheKey4);
    Assertions.assertEquals(entityCacheKey1, entityCacheKey5);

    Assertions.assertNotEquals(entityCacheKey1.hashCode(), entityCacheKey2.hashCode());
    Assertions.assertNotEquals(entityCacheKey1.hashCode(), entityCacheKey3.hashCode());
    Assertions.assertNotEquals(entityCacheKey1.hashCode(), entityCacheKey4.hashCode());
    Assertions.assertNotEquals(entityCacheKey2.hashCode(), entityCacheKey3.hashCode());
    Assertions.assertNotEquals(entityCacheKey2.hashCode(), entityCacheKey4.hashCode());
    Assertions.assertNotEquals(entityCacheKey3.hashCode(), entityCacheKey4.hashCode());
    Assertions.assertEquals(entityCacheKey1.hashCode(), entityCacheKey5.hashCode());

    Assertions.assertEquals("m1.c1.s1.t1.mo", entityCacheKey1.toString());
  }
}
