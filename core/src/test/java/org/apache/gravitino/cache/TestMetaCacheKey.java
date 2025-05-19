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

public class TestMetaCacheKey {

  @Test
  void testCreateMetaCacheKeyUseConstructor() {
    NameIdentifier ident = NameIdentifier.of("m1.c1.s1.t1");
    MetaCacheKey metaCacheKey = new MetaCacheKey(ident, Entity.EntityType.MODEL);
    Assertions.assertEquals(ident, metaCacheKey.identifier());
    Assertions.assertEquals(Entity.EntityType.MODEL, metaCacheKey.type());
  }

  @Test
  void testCreateMetaCacheKeyUseStaticMethod() {
    NameIdentifier ident = NameIdentifier.of("m1.c1.s1.t1");
    MetaCacheKey metaCacheKey = MetaCacheKey.of(ident, Entity.EntityType.MODEL);
    Assertions.assertEquals(ident, metaCacheKey.identifier());
    Assertions.assertEquals(Entity.EntityType.MODEL, metaCacheKey.type());
  }

  @Test
  void testCreateMetaCacheKeyWithNullArguments() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> MetaCacheKey.of(null, Entity.EntityType.MODEL));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> MetaCacheKey.of(NameIdentifier.of("m1.c1.s1.t1"), null));
    Assertions.assertThrows(IllegalArgumentException.class, () -> MetaCacheKey.of(null, null));
  }

  @Test
  void testOtherMethods() {
    NameIdentifier ident1 = NameIdentifier.of("m1.c1.s1.t1");
    NameIdentifier ident2 = NameIdentifier.of("m1.c1.s1.t2");
    NameIdentifier ident3 = NameIdentifier.of("m1.c1.s1.t1");

    MetaCacheKey metaCacheKey1 = MetaCacheKey.of(ident1, Entity.EntityType.MODEL);
    MetaCacheKey metaCacheKey2 = MetaCacheKey.of(ident2, Entity.EntityType.MODEL);
    MetaCacheKey metaCacheKey3 = MetaCacheKey.of(ident1, Entity.EntityType.TABLE);
    MetaCacheKey metaCacheKey4 = MetaCacheKey.of(ident2, Entity.EntityType.TABLE);
    MetaCacheKey metaCacheKey5 = MetaCacheKey.of(ident3, Entity.EntityType.MODEL);

    Assertions.assertNotEquals(metaCacheKey1, metaCacheKey2);
    Assertions.assertNotEquals(metaCacheKey1, metaCacheKey3);
    Assertions.assertNotEquals(metaCacheKey1, metaCacheKey4);
    Assertions.assertNotEquals(metaCacheKey2, metaCacheKey3);
    Assertions.assertNotEquals(metaCacheKey2, metaCacheKey4);
    Assertions.assertNotEquals(metaCacheKey3, metaCacheKey4);
    Assertions.assertEquals(metaCacheKey1, metaCacheKey5);

    Assertions.assertNotEquals(metaCacheKey1.hashCode(), metaCacheKey2.hashCode());
    Assertions.assertNotEquals(metaCacheKey1.hashCode(), metaCacheKey3.hashCode());
    Assertions.assertNotEquals(metaCacheKey1.hashCode(), metaCacheKey4.hashCode());
    Assertions.assertNotEquals(metaCacheKey2.hashCode(), metaCacheKey3.hashCode());
    Assertions.assertNotEquals(metaCacheKey2.hashCode(), metaCacheKey4.hashCode());
    Assertions.assertNotEquals(metaCacheKey3.hashCode(), metaCacheKey4.hashCode());
    Assertions.assertEquals(metaCacheKey1.hashCode(), metaCacheKey5.hashCode());

    Assertions.assertEquals("m1.c1.s1.t1.mo", metaCacheKey1.toString());
  }
}
