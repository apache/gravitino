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

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.cache.GravitinoCache;
import org.apache.gravitino.storage.relational.po.auth.OwnerInfo;
import org.apache.gravitino.storage.relational.po.cache.EntityChangeRecord;
import org.apache.gravitino.storage.relational.po.cache.OperateType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests for {@link JcasbinChangeListener} static helpers. */
public class TestJcasbinChangePoller {

  @Test
  void testRejectsNonPositivePollInterval() {
    RecordingCache<String, Long> metadataIdCache = new RecordingCache<>();
    RecordingCache<Long, Optional<OwnerInfo>> ownerRelCache = new RecordingCache<>();

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new JcasbinChangeListener(metadataIdCache, ownerRelCache, 0));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new JcasbinChangeListener(metadataIdCache, ownerRelCache, -1));
  }

  @Test
  void testChangeLogFullNameStripsLeadingMetalakeForChildTypes() {
    MetadataObject catalog =
        JcasbinChangeListener.metadataObjectFromChangeLog(
            "ml1", "ml1.cat1", MetadataObject.Type.CATALOG);
    Assertions.assertEquals(
        key("ml1", "CATALOG", "cat1", ""),
        JcasbinAuthorizationCacheKeys.metadataIdCacheKey("ml1", catalog));

    MetadataObject schema =
        JcasbinChangeListener.metadataObjectFromChangeLog(
            "ml1", "ml1.cat1.sch1", MetadataObject.Type.SCHEMA);
    Assertions.assertEquals(
        key("ml1", "CATALOG", "cat1", "SCHEMA", "sch1", ""),
        JcasbinAuthorizationCacheKeys.metadataIdCacheKey("ml1", schema));

    MetadataObject table =
        JcasbinChangeListener.metadataObjectFromChangeLog(
            "ml1", "ml1.cat1.sch1.tbl1", MetadataObject.Type.TABLE);
    Assertions.assertEquals(
        key("ml1", "CATALOG", "cat1", "SCHEMA", "sch1", "TABLE", "tbl1", ""),
        JcasbinAuthorizationCacheKeys.metadataIdCacheKey("ml1", table));
  }

  @Test
  void testChangeLogFullNameForMetalakeKeepsItself() {
    MetadataObject metalake =
        JcasbinChangeListener.metadataObjectFromChangeLog(
            "ml1", "ml1", MetadataObject.Type.METALAKE);
    Assertions.assertEquals(
        key("ml1", "METALAKE", ""),
        JcasbinAuthorizationCacheKeys.metadataIdCacheKey("ml1", metalake));
  }

  @Test
  void testPollEntityChangesCoalescesContainerPrefixes() {
    RecordingCache<String, Long> metadataIdCache = new RecordingCache<>();
    RecordingCache<Long, Optional<OwnerInfo>> ownerRelCache = new RecordingCache<>();

    JcasbinChangeListener poller = new JcasbinChangeListener(metadataIdCache, ownerRelCache, 1);
    poller.onEntityChange(
        List.of(
            change(1L, MetadataObject.Type.CATALOG, "ml1.cat1"),
            change(2L, MetadataObject.Type.SCHEMA, "ml1.cat1.sch1"),
            change(3L, MetadataObject.Type.TABLE, "ml1.cat1.sch1.tbl1"),
            change(4L, MetadataObject.Type.TABLE, "ml1.cat2.sch1.tbl1")));

    Assertions.assertEquals(
        List.of(
            key("ml1", "CATALOG", "cat1", ""),
            key("ml1", "CATALOG", "cat2", "SCHEMA", "sch1", "TABLE", "tbl1", "")),
        metadataIdCache.invalidatedPrefixes);
    Assertions.assertEquals(List.of(), metadataIdCache.invalidatedKeys);
  }

  @Test
  void testPollCursorAdvancementIsSynchronized() throws NoSuchMethodException {
    Method pollOwnerChanges = JcasbinChangeListener.class.getDeclaredMethod("pollOwnerChanges");
    Method onEntityChange =
        JcasbinChangeListener.class.getDeclaredMethod("onEntityChange", List.class);

    Assertions.assertTrue(Modifier.isSynchronized(pollOwnerChanges.getModifiers()));
    Assertions.assertTrue(Modifier.isSynchronized(onEntityChange.getModifiers()));
  }

  private static String key(String... parts) {
    return JcasbinAuthorizationCacheKeys.joinKeyParts(parts);
  }

  private static EntityChangeRecord change(long id, MetadataObject.Type type, String fullName) {
    return new EntityChangeRecord(id, "ml1", type.name(), fullName, OperateType.ALTER, 0L);
  }

  private static class RecordingCache<K, V> implements GravitinoCache<K, V> {
    private final List<K> invalidatedKeys = new ArrayList<>();
    private final List<String> invalidatedPrefixes = new ArrayList<>();

    @Override
    public Optional<V> getIfPresent(K key) {
      return Optional.empty();
    }

    @Override
    public void put(K key, V value) {}

    @Override
    public void invalidate(K key) {
      invalidatedKeys.add(key);
    }

    @Override
    public void invalidateAll() {}

    @Override
    public void invalidateByPrefix(String prefix) {
      invalidatedPrefixes.add(prefix);
    }

    @Override
    public long size() {
      return 0;
    }

    @Override
    public void close() {}
  }
}
