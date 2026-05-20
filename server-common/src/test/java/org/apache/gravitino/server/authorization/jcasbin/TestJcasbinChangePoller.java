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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.cache.GravitinoCache;
import org.apache.gravitino.storage.relational.mapper.EntityChangeLogMapper;
import org.apache.gravitino.storage.relational.mapper.OwnerMetaMapper;
import org.apache.gravitino.storage.relational.po.auth.OwnerInfo;
import org.apache.gravitino.storage.relational.po.cache.EntityChangeRecord;
import org.apache.gravitino.storage.relational.po.cache.OperateType;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

/** Tests for {@link JcasbinChangePoller} static helpers. */
public class TestJcasbinChangePoller {

  @Test
  void testRejectsNonPositivePollInterval() {
    RecordingCache<String, Long> metadataIdCache = new RecordingCache<>();
    RecordingCache<Long, Optional<OwnerInfo>> ownerRelCache = new RecordingCache<>();

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new JcasbinChangePoller(metadataIdCache, ownerRelCache, 0));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new JcasbinChangePoller(metadataIdCache, ownerRelCache, -1));
  }

  @Test
  void testChangeLogFullNameStripsLeadingMetalakeForChildTypes() {
    MetadataObject catalog =
        JcasbinChangePoller.metadataObjectFromChangeLog(
            "ml1", "ml1.cat1", MetadataObject.Type.CATALOG);
    Assertions.assertEquals(
        key("ml1", "cat1", ""), JcasbinAuthorizationCacheKeys.metadataObjectKey("ml1", catalog));

    MetadataObject schema =
        JcasbinChangePoller.metadataObjectFromChangeLog(
            "ml1", "ml1.cat1.sch1", MetadataObject.Type.SCHEMA);
    Assertions.assertEquals(
        key("ml1", "cat1", "sch1", ""),
        JcasbinAuthorizationCacheKeys.metadataObjectKey("ml1", schema));

    MetadataObject table =
        JcasbinChangePoller.metadataObjectFromChangeLog(
            "ml1", "ml1.cat1.sch1.tbl1", MetadataObject.Type.TABLE);
    Assertions.assertEquals(
        key("ml1", "cat1", "sch1", "tbl1", "TABLE"),
        JcasbinAuthorizationCacheKeys.metadataObjectKey("ml1", table));
  }

  @Test
  void testChangeLogFullNameForMetalakeKeepsItself() {
    MetadataObject metalake =
        JcasbinChangePoller.metadataObjectFromChangeLog("ml1", "ml1", MetadataObject.Type.METALAKE);
    Assertions.assertEquals(
        key("ml1", ""), JcasbinAuthorizationCacheKeys.metadataObjectKey("ml1", metalake));
  }

  @Test
  void testPollEntityChangesCoalescesContainerPrefixes() {
    RecordingCache<String, Long> metadataIdCache = new RecordingCache<>();
    RecordingCache<Long, Optional<OwnerInfo>> ownerRelCache = new RecordingCache<>();
    EntityChangeLogMapper entityChangeLogMapper = mock(EntityChangeLogMapper.class);
    OwnerMetaMapper ownerMetaMapper = mock(OwnerMetaMapper.class);

    when(ownerMetaMapper.selectChangedOwners(0L)).thenReturn(Collections.emptyList());
    when(entityChangeLogMapper.selectEntityChanges(0L, 500))
        .thenReturn(
            List.of(
                change(1L, MetadataObject.Type.CATALOG, "ml1.cat1"),
                change(2L, MetadataObject.Type.SCHEMA, "ml1.cat1.sch1"),
                change(3L, MetadataObject.Type.TABLE, "ml1.cat1.sch1.tbl1"),
                change(4L, MetadataObject.Type.TABLE, "ml1.cat2.sch1.tbl1")));

    try (MockedStatic<SessionUtils> sessionUtils = mockStatic(SessionUtils.class)) {
      sessionUtils
          .when(() -> SessionUtils.getWithoutCommit(any(), any()))
          .thenAnswer(
              invocation -> {
                Class<?> mapperClass = invocation.getArgument(0);
                Function<Object, Object> func = invocation.getArgument(1);
                if (mapperClass == OwnerMetaMapper.class) {
                  return func.apply(ownerMetaMapper);
                }
                if (mapperClass == EntityChangeLogMapper.class) {
                  return func.apply(entityChangeLogMapper);
                }
                return null;
              });

      JcasbinChangePoller poller = new JcasbinChangePoller(metadataIdCache, ownerRelCache, 1);
      poller.pollChanges();
    }

    Assertions.assertEquals(List.of(key("ml1", "cat1", "")), metadataIdCache.invalidatedPrefixes);
    Assertions.assertEquals(
        List.of(key("ml1", "cat2", "sch1", "tbl1", "TABLE")), metadataIdCache.invalidatedKeys);
  }

  @Test
  void testPollCursorAdvancementIsSynchronized() throws NoSuchMethodException {
    Method pollOwnerChanges = JcasbinChangePoller.class.getDeclaredMethod("pollOwnerChanges");
    Method pollEntityChanges = JcasbinChangePoller.class.getDeclaredMethod("pollEntityChanges");

    Assertions.assertTrue(Modifier.isSynchronized(pollOwnerChanges.getModifiers()));
    Assertions.assertTrue(Modifier.isSynchronized(pollEntityChanges.getModifiers()));
  }

  private static String key(String... parts) {
    return String.join(JcasbinAuthorizationCacheKeys.SEPARATOR, parts);
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
