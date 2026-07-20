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
package org.apache.gravitino.storage.relational;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.List;
import org.apache.gravitino.Config;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.cache.CaffeineEntityCache;
import org.apache.gravitino.cache.Coherence;
import org.apache.gravitino.cache.EntityCache;
import org.apache.gravitino.cache.NoOpsCache;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.storage.relational.po.cache.EntityChangeRecord;
import org.apache.gravitino.storage.relational.po.cache.OperateType;
import org.apache.gravitino.utils.TestUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests {@link EntityCacheChangeLogListener}: parsing, dispatch, hierarchical cascade. */
public class TestEntityCacheChangeLogListener {

  private static EntityChangeRecord record(
      EntityType type, String fullName, OperateType operateType) {
    return new EntityChangeRecord(
        1L, "metalake", type == null ? null : type.name(), fullName, operateType, 0L);
  }

  @Test
  void testDropInvalidatesExactlyTheChangedKey() {
    CaffeineEntityCache cache = new CaffeineEntityCache(new Config() {});
    CatalogEntity catalog =
        TestUtil.getTestCatalogEntity(1L, "catalog1", Namespace.of("metalake"), "hive", "cmt");
    SchemaEntity schema =
        TestUtil.getTestSchemaEntity(2L, "schema1", Namespace.of("metalake", "catalog1"), "cmt");
    cache.put(catalog);
    cache.put(schema);

    EntityCacheChangeLogListener listener = new EntityCacheChangeLogListener(cache);
    listener.onEntityChange(
        List.of(record(EntityType.SCHEMA, schema.nameIdentifier().toString(), OperateType.DROP)));

    Assertions.assertFalse(cache.contains(schema.nameIdentifier(), EntityType.SCHEMA));
    Assertions.assertTrue(cache.contains(catalog.nameIdentifier(), EntityType.CATALOG));
  }

  @Test
  void testContainerDropCascadesToChildrenButKeepsSiblings() {
    CaffeineEntityCache cache = new CaffeineEntityCache(new Config() {});
    CatalogEntity catalog =
        TestUtil.getTestCatalogEntity(1L, "catalog1", Namespace.of("metalake"), "hive", "cmt");
    SchemaEntity schema1 =
        TestUtil.getTestSchemaEntity(2L, "schema1", Namespace.of("metalake", "catalog1"), "cmt");
    TableEntity table1 =
        TestUtil.getTestTableEntity(3L, "table1", Namespace.of("metalake", "catalog1", "schema1"));
    SchemaEntity schema2 =
        TestUtil.getTestSchemaEntity(4L, "schema2", Namespace.of("metalake", "catalog1"), "cmt");
    cache.put(catalog);
    cache.put(schema1);
    cache.put(table1);
    cache.put(schema2);
    Assertions.assertEquals(4, cache.size());

    EntityCacheChangeLogListener listener = new EntityCacheChangeLogListener(cache);
    listener.onEntityChange(
        List.of(record(EntityType.SCHEMA, schema1.nameIdentifier().toString(), OperateType.DROP)));

    // schema1 and its child table1 are dropped through the forward prefix scan ...
    Assertions.assertFalse(cache.contains(schema1.nameIdentifier(), EntityType.SCHEMA));
    Assertions.assertFalse(cache.contains(table1.nameIdentifier(), EntityType.TABLE));
    // ... while the sibling schema2 and the parent catalog stay cached.
    Assertions.assertTrue(cache.contains(schema2.nameIdentifier(), EntityType.SCHEMA));
    Assertions.assertTrue(cache.contains(catalog.nameIdentifier(), EntityType.CATALOG));
  }

  @Test
  void testAlterInvalidatesTagKey() {
    CaffeineEntityCache cache = new CaffeineEntityCache(new Config() {});
    TagEntity tag = TestUtil.getTestTagEntity(1L, "tag1", Namespace.of("m1"), "comment");
    cache.put(tag);

    EntityCacheChangeLogListener listener = new EntityCacheChangeLogListener(cache);
    listener.onEntityChange(
        List.of(record(EntityType.TAG, tag.nameIdentifier().toString(), OperateType.ALTER)));

    Assertions.assertFalse(cache.contains(tag.nameIdentifier(), EntityType.TAG));
  }

  @Test
  void testReplaysEveryCacheableTypeAsInvalidate() {
    EntityCache cache = mock(EntityCache.class);
    EntityCacheChangeLogListener listener = new EntityCacheChangeLogListener(cache);

    listener.onEntityChange(
        List.of(
            record(EntityType.CATALOG, "m1.cat1", OperateType.ALTER),
            record(EntityType.SCHEMA, "m1.cat1.sch1", OperateType.DROP),
            record(EntityType.TABLE, "m1.cat1.sch1.tbl1", OperateType.DROP),
            record(EntityType.TAG, "m1.system.tag.tag1", OperateType.ALTER),
            record(EntityType.POLICY, "m1.system.policy.p1", OperateType.DROP),
            record(EntityType.JOB, "m1.system.job.job-1", OperateType.ALTER)));

    verify(cache).invalidate(NameIdentifier.of("m1", "cat1"), EntityType.CATALOG);
    verify(cache).invalidate(NameIdentifier.of("m1", "cat1", "sch1"), EntityType.SCHEMA);
    verify(cache).invalidate(NameIdentifier.of("m1", "cat1", "sch1", "tbl1"), EntityType.TABLE);
    verify(cache).invalidate(NameIdentifier.of("m1", "system", "tag", "tag1"), EntityType.TAG);
    verify(cache).invalidate(NameIdentifier.of("m1", "system", "policy", "p1"), EntityType.POLICY);
    verify(cache).invalidate(NameIdentifier.of("m1", "system", "job", "job-1"), EntityType.JOB);
  }

  @Test
  void testSkipsMalformedRowsAndContinues() {
    EntityCache cache = mock(EntityCache.class);
    EntityCacheChangeLogListener listener = new EntityCacheChangeLogListener(cache);

    listener.onEntityChange(
        List.of(
            record(null, "m1.cat1", OperateType.ALTER),
            record(EntityType.CATALOG, null, OperateType.ALTER),
            new EntityChangeRecord(1L, "m1", "NOT_A_TYPE", "m1.x", OperateType.ALTER, 0L),
            record(EntityType.CATALOG, "m1.cat_ok", OperateType.ALTER)));

    // Only the single well-formed row is applied.
    verify(cache, times(1)).invalidate(any(NameIdentifier.class), any(EntityType.class));
    verify(cache).invalidate(NameIdentifier.of("m1", "cat_ok"), EntityType.CATALOG);
  }

  @Test
  void testOneFailingRowDoesNotStopTheRest() {
    EntityCache cache = mock(EntityCache.class);
    NameIdentifier failing = NameIdentifier.of("m1", "boom");
    doThrow(new RuntimeException("boom")).when(cache).invalidate(failing, EntityType.CATALOG);

    EntityCacheChangeLogListener listener = new EntityCacheChangeLogListener(cache);
    listener.onEntityChange(
        List.of(
            record(EntityType.CATALOG, "m1.boom", OperateType.DROP),
            record(EntityType.CATALOG, "m1.ok", OperateType.DROP)));

    verify(cache).invalidate(NameIdentifier.of("m1", "ok"), EntityType.CATALOG);
  }

  @Test
  void testCaffeineAndNoOpsCachesAreLocalPerNode() {
    Assertions.assertEquals(
        Coherence.LOCAL_PER_NODE, new CaffeineEntityCache(new Config() {}).coherence());
    Assertions.assertEquals(Coherence.LOCAL_PER_NODE, new NoOpsCache(new Config() {}).coherence());
  }
}
