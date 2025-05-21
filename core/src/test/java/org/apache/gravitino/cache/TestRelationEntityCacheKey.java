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
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestRelationEntityCacheKey {

  @Test
  void testCreateRelationEntityCacheKeyUsingConstructor() {
    NameIdentifier ident = NameIdentifierUtil.ofRole("metalake", "role1");
    RelationEntityCacheKey key =
        new RelationEntityCacheKey(
            ident, Entity.EntityType.ROLE, SupportsRelationOperations.Type.ROLE_GROUP_REL);
    Assertions.assertEquals("metalake.system.role.role1:ro:ROLE_GROUP_REL", key.toString());
    Assertions.assertEquals(
        NameIdentifier.of("metalake", "system", "role", "role1"), key.identifier());
    Assertions.assertEquals(Entity.EntityType.ROLE, key.entityType());
    Assertions.assertEquals(SupportsRelationOperations.Type.ROLE_GROUP_REL, key.relationType());
    Assertions.assertEquals(
        key.storeEntityCacheKey(), StoreEntityCacheKey.of(ident, Entity.EntityType.ROLE));
  }

  @Test
  void testCreateRelationEntityCacheKeyUsingStaticMethod() {
    NameIdentifier ident = NameIdentifierUtil.ofRole("metalake", "role1");
    RelationEntityCacheKey key =
        RelationEntityCacheKey.of(
            ident, Entity.EntityType.ROLE, SupportsRelationOperations.Type.ROLE_GROUP_REL);
    Assertions.assertEquals("metalake.system.role.role1:ro:ROLE_GROUP_REL", key.toString());
    Assertions.assertEquals(
        NameIdentifier.of("metalake", "system", "role", "role1"), key.identifier());
    Assertions.assertEquals(Entity.EntityType.ROLE, key.entityType());
    Assertions.assertEquals(SupportsRelationOperations.Type.ROLE_GROUP_REL, key.relationType());
    Assertions.assertEquals(
        key.storeEntityCacheKey(), StoreEntityCacheKey.of(ident, Entity.EntityType.ROLE));
  }

  @Test
  void testCreateRelationEntityCacheKeyWithNullArguments() {
    NameIdentifier ident = NameIdentifierUtil.ofRole("metalake", "role1");
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          RelationEntityCacheKey.of(
              null, Entity.EntityType.ROLE, SupportsRelationOperations.Type.ROLE_GROUP_REL);
        });
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          RelationEntityCacheKey.of(ident, null, SupportsRelationOperations.Type.ROLE_GROUP_REL);
        });
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          RelationEntityCacheKey.of(ident, Entity.EntityType.ROLE, null);
        });
  }

  @Test
  public void testEqualsAndHashCodeWithSameValues() {
    NameIdentifier ident = NameIdentifier.of("catalog", "schema");
    Entity.EntityType type = Entity.EntityType.SCHEMA;
    SupportsRelationOperations.Type relType = SupportsRelationOperations.Type.OWNER_REL;

    RelationEntityCacheKey key1 = new RelationEntityCacheKey(ident, type, relType);
    RelationEntityCacheKey key2 = new RelationEntityCacheKey(ident, type, relType);

    Assertions.assertEquals(key1, key2);
    Assertions.assertEquals(key1.hashCode(), key2.hashCode());
  }

  @Test
  public void testEqualsWithDifferentIdentifiers() {
    NameIdentifier ident1 = NameIdentifier.of("catalog", "schema1");
    NameIdentifier ident2 = NameIdentifier.of("catalog", "schema2");

    RelationEntityCacheKey key1 =
        new RelationEntityCacheKey(
            ident1, Entity.EntityType.SCHEMA, SupportsRelationOperations.Type.OWNER_REL);
    RelationEntityCacheKey key2 =
        new RelationEntityCacheKey(
            ident2, Entity.EntityType.SCHEMA, SupportsRelationOperations.Type.OWNER_REL);

    Assertions.assertNotEquals(key1, key2);
  }

  @Test
  public void testEqualsWithDifferentTypes() {
    NameIdentifier ident = NameIdentifier.of("catalog", "schema");

    RelationEntityCacheKey key1 =
        new RelationEntityCacheKey(
            ident, Entity.EntityType.ROLE, SupportsRelationOperations.Type.OWNER_REL);
    RelationEntityCacheKey key2 =
        new RelationEntityCacheKey(
            ident, Entity.EntityType.USER, SupportsRelationOperations.Type.OWNER_REL);

    Assertions.assertNotEquals(key1, key2);
  }

  @Test
  public void testEquals_differentRelationTypes() {
    NameIdentifier ident = NameIdentifier.of("catalog", "schema");

    RelationEntityCacheKey key1 =
        new RelationEntityCacheKey(
            ident, Entity.EntityType.ROLE, SupportsRelationOperations.Type.OWNER_REL);
    RelationEntityCacheKey key2 =
        new RelationEntityCacheKey(
            ident, Entity.EntityType.ROLE, SupportsRelationOperations.Type.ROLE_USER_REL);

    Assertions.assertNotEquals(key1, key2);
  }
}
