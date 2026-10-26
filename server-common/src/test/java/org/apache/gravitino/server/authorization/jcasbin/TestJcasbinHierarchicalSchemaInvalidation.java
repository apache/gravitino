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
package org.apache.gravitino.server.authorization.jcasbin;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Optional;
import org.apache.gravitino.Entity;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.cache.CaffeineGravitinoCache;
import org.apache.gravitino.cache.GravitinoCache;
import org.apache.gravitino.storage.relational.EntityChangeLogNameIdentifierCodec;
import org.apache.gravitino.storage.relational.po.auth.OwnerInfo;
import org.apache.gravitino.storage.relational.po.cache.EntityChangeRecord;
import org.apache.gravitino.storage.relational.po.cache.OperateType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Checks that dropping a schema evicts the name-to-id entries of its hierarchical descendants
 * ({@code A:B}, {@code A:B:C}) and of the tables and views under them.
 */
public class TestJcasbinHierarchicalSchemaInvalidation {

  private static final String METALAKE = "ml1";
  private static final String CATALOG = "cat1";

  private GravitinoCache<String, Long> metadataIdCache;

  @BeforeEach
  public void setUp() {
    metadataIdCache = new CaffeineGravitinoCache<>(3_600_000L, 1000L);
    put(MetadataObjects.of(CATALOG, "A", MetadataObject.Type.SCHEMA), 1L);
    put(MetadataObjects.of(CATALOG, "A:B", MetadataObject.Type.SCHEMA), 2L);
    put(MetadataObjects.of(CATALOG, "A:B:C", MetadataObject.Type.SCHEMA), 3L);
    put(MetadataObjects.parse(CATALOG + ".A:B.t", MetadataObject.Type.TABLE), 4L);
    put(MetadataObjects.parse(CATALOG + ".A:B.v", MetadataObject.Type.VIEW), 5L);
    // Siblings that share a string prefix with "A" but are not its descendants.
    put(MetadataObjects.of(CATALOG, "AB", MetadataObject.Type.SCHEMA), 6L);
    put(MetadataObjects.parse(CATALOG + ".AB.t", MetadataObject.Type.TABLE), 7L);
  }

  @Test
  public void testLocalSchemaDropEvictsHierarchicalDescendants() throws Exception {
    JcasbinAuthorizer authorizer = new JcasbinAuthorizer();
    Field field = JcasbinAuthorizer.class.getDeclaredField("metadataIdCache");
    field.setAccessible(true);
    field.set(authorizer, metadataIdCache);

    authorizer.handleEntityNameIdMappingChange(
        METALAKE, NameIdentifier.of(METALAKE, CATALOG, "A"), Entity.EntityType.SCHEMA);

    assertDescendantsEvictedAndSiblingsKept();
  }

  @Test
  public void testChangeLogSchemaDropEvictsHierarchicalDescendants() {
    GravitinoCache<Long, Optional<OwnerInfo>> ownerRelCache =
        new CaffeineGravitinoCache<>(3_600_000L, 1000L);
    JcasbinChangeListener listener = new JcasbinChangeListener(metadataIdCache, ownerRelCache, 1);

    // A cascade drop of "A" writes a single DROP row for "A" only (JDBCBackend.delete).
    String fullName =
        EntityChangeLogNameIdentifierCodec.encode(NameIdentifier.of(METALAKE, CATALOG, "A"));
    listener.onEntityChange(
        List.of(
            new EntityChangeRecord(
                1L, METALAKE, MetadataObject.Type.SCHEMA.name(), fullName, OperateType.DROP, 0L)));

    assertDescendantsEvictedAndSiblingsKept();
  }

  private void assertDescendantsEvictedAndSiblingsKept() {
    assertEvicted(MetadataObjects.of(CATALOG, "A", MetadataObject.Type.SCHEMA));
    assertEvicted(MetadataObjects.of(CATALOG, "A:B", MetadataObject.Type.SCHEMA));
    assertEvicted(MetadataObjects.of(CATALOG, "A:B:C", MetadataObject.Type.SCHEMA));
    assertEvicted(MetadataObjects.parse(CATALOG + ".A:B.t", MetadataObject.Type.TABLE));
    assertEvicted(MetadataObjects.parse(CATALOG + ".A:B.v", MetadataObject.Type.VIEW));

    assertKept(MetadataObjects.of(CATALOG, "AB", MetadataObject.Type.SCHEMA));
    assertKept(MetadataObjects.parse(CATALOG + ".AB.t", MetadataObject.Type.TABLE));
  }

  private void put(MetadataObject object, long id) {
    metadataIdCache.put(JcasbinAuthorizationCacheKeys.metadataIdCacheKey(METALAKE, object), id);
  }

  private void assertEvicted(MetadataObject object) {
    Assertions.assertFalse(
        metadataIdCache
            .getIfPresent(JcasbinAuthorizationCacheKeys.metadataIdCacheKey(METALAKE, object))
            .isPresent(),
        "stale name-to-id entry survived the drop of schema A: " + object.fullName());
  }

  private void assertKept(MetadataObject object) {
    Assertions.assertTrue(
        metadataIdCache
            .getIfPresent(JcasbinAuthorizationCacheKeys.metadataIdCacheKey(METALAKE, object))
            .isPresent(),
        "unrelated entry was evicted: " + object.fullName());
  }
}
