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
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.apache.gravitino.storage.relational.mapper.EntityChangeLogMapper;
import org.apache.gravitino.storage.relational.po.cache.EntityChangeRecord;
import org.apache.gravitino.storage.relational.po.cache.OperateType;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

public class TestEntityChangeLogPoller {

  @Test
  void testRejectsNonPositivePollInterval() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> new EntityChangeLogPoller(0));
    Assertions.assertThrows(IllegalArgumentException.class, () -> new EntityChangeLogPoller(-1));
  }

  @Test
  void testPollChangesDispatchesSameBatchToAllListenersAndAdvancesCursor() {
    EntityChangeLogMapper mapper = mock(EntityChangeLogMapper.class);
    EntityChangeRecord first = change(1L, "CATALOG", "ml1.cat1");
    EntityChangeRecord second = change(2L, "SCHEMA", "ml1.cat1.sch1");
    when(mapper.selectEntityChanges(0L, 2000)).thenReturn(List.of(first, second));
    when(mapper.selectEntityChanges(2L, 2000)).thenReturn(List.of());

    List<EntityChangeRecord> firstListenerRecords = new ArrayList<>();
    List<EntityChangeRecord> secondListenerRecords = new ArrayList<>();

    try (MockedStatic<SessionUtils> sessionUtils = mockStatic(SessionUtils.class)) {
      sessionUtils
          .when(() -> SessionUtils.getWithoutCommit(any(), any()))
          .thenAnswer(
              invocation -> {
                Function<Object, Object> func = invocation.getArgument(1);
                return func.apply(mapper);
              });

      EntityChangeLogPoller poller = new EntityChangeLogPoller(1);
      poller.registerListener(firstListenerRecords::addAll);
      poller.registerListener(secondListenerRecords::addAll);

      poller.pollChanges();
      poller.pollChanges();
    }

    Assertions.assertEquals(List.of(first, second), firstListenerRecords);
    Assertions.assertEquals(List.of(first, second), secondListenerRecords);
  }

  @Test
  void testRetriesOnlyFailedListenersBeforeAdvancingCursor() {
    EntityChangeLogMapper mapper = mock(EntityChangeLogMapper.class);
    EntityChangeRecord change = change(1L, "CATALOG", "ml1.cat1");
    when(mapper.selectEntityChanges(0L, 2000)).thenReturn(List.of(change));
    when(mapper.selectEntityChanges(1L, 2000)).thenReturn(List.of());

    List<EntityChangeRecord> received = new ArrayList<>();
    AtomicInteger failingListenerCalls = new AtomicInteger();
    AtomicBoolean firstCall = new AtomicBoolean(true);

    try (MockedStatic<SessionUtils> sessionUtils = mockStatic(SessionUtils.class)) {
      sessionUtils
          .when(() -> SessionUtils.getWithoutCommit(any(), any()))
          .thenAnswer(
              invocation -> {
                Function<Object, Object> func = invocation.getArgument(1);
                return func.apply(mapper);
              });

      EntityChangeLogPoller poller = new EntityChangeLogPoller(1);
      poller.registerListener(
          changes -> {
            failingListenerCalls.incrementAndGet();
            if (firstCall.getAndSet(false)) {
              throw new RuntimeException("listener failed");
            }
          });
      poller.registerListener(received::addAll);

      poller.pollChanges();
      poller.pollChanges();
      poller.pollChanges();
    }

    Assertions.assertEquals(2, failingListenerCalls.get());
    Assertions.assertEquals(List.of(change), received);
    verify(mapper).selectEntityChanges(0L, 2000);
    verify(mapper).selectEntityChanges(1L, 2000);
  }

  @Test
  void testUnregisteredFailedListenerDoesNotBlockCursor() {
    EntityChangeLogMapper mapper = mock(EntityChangeLogMapper.class);
    EntityChangeRecord change = change(1L, "CATALOG", "ml1.cat1");
    when(mapper.selectEntityChanges(0L, 2000)).thenReturn(List.of(change));
    when(mapper.selectEntityChanges(1L, 2000)).thenReturn(List.of());

    try (MockedStatic<SessionUtils> sessionUtils = mockStatic(SessionUtils.class)) {
      sessionUtils
          .when(() -> SessionUtils.getWithoutCommit(any(), any()))
          .thenAnswer(
              invocation -> {
                Function<Object, Object> func = invocation.getArgument(1);
                return func.apply(mapper);
              });

      EntityChangeLogPoller poller = new EntityChangeLogPoller(1);
      EntityChangeLogListener failedListener =
          changes -> {
            throw new RuntimeException("listener failed");
          };
      poller.registerListener(failedListener);

      poller.pollChanges();
      poller.unregisterListener(failedListener);
      poller.pollChanges();
      poller.pollChanges();
    }

    verify(mapper).selectEntityChanges(1L, 2000);
  }

  @Test
  void testDoesNotPruneWhileListenerFailurePausesCursor() {
    EntityChangeLogMapper mapper = mock(EntityChangeLogMapper.class);
    EntityChangeRecord change = change(1L, "CATALOG", "ml1.cat1");
    when(mapper.selectEntityChanges(0L, 2000)).thenReturn(List.of(change));

    try (MockedStatic<SessionUtils> sessionUtils = mockStatic(SessionUtils.class)) {
      mockSessionUtils(sessionUtils, mapper);

      EntityChangeLogPoller poller = new EntityChangeLogPoller(1);
      poller.registerListener(
          changes -> {
            throw new RuntimeException("listener failed");
          });

      poller.pollChanges();
      poller.pollChanges();
    }

    verify(mapper).selectEntityChanges(0L, 2000);
    verify(mapper, never()).pruneOldEntityChanges(anyLong());
  }

  @Test
  void testDoesNotPruneUntilBacklogLargerThanOneBatchIsConsumed() {
    EntityChangeLogMapper mapper = mock(EntityChangeLogMapper.class);
    List<EntityChangeRecord> firstBatch = changes(1L, 2000L);
    EntityChangeRecord remainingChange = change(2001L, "TABLE", "ml1.cat1.schema1.table2001");
    when(mapper.selectEntityChanges(0L, 2000)).thenReturn(firstBatch);
    when(mapper.selectEntityChanges(2000L, 2000)).thenReturn(List.of(remainingChange));

    List<EntityChangeRecord> received = new ArrayList<>();
    try (MockedStatic<SessionUtils> sessionUtils = mockStatic(SessionUtils.class)) {
      mockSessionUtils(sessionUtils, mapper);

      EntityChangeLogPoller poller = new EntityChangeLogPoller(1);
      poller.registerListener(received::addAll);

      poller.pollChanges();
      verify(mapper, never()).pruneOldEntityChanges(anyLong());

      poller.pollChanges();
    }

    Assertions.assertEquals(2001, received.size());
    verify(mapper, never()).pruneOldEntityChanges(anyLong());
  }

  @Test
  void testPollChangesCatchesFetchFailures() {
    EntityChangeLogMapper mapper = mock(EntityChangeLogMapper.class);
    when(mapper.selectEntityChanges(0L, 2000)).thenThrow(new RuntimeException("db failed"));

    try (MockedStatic<SessionUtils> sessionUtils = mockStatic(SessionUtils.class)) {
      sessionUtils
          .when(() -> SessionUtils.getWithoutCommit(any(), any()))
          .thenAnswer(
              invocation -> {
                Function<Object, Object> func = invocation.getArgument(1);
                return func.apply(mapper);
              });

      EntityChangeLogPoller poller = new EntityChangeLogPoller(1);

      Assertions.assertDoesNotThrow(poller::pollChanges);
    }
  }

  @Test
  void testDispatchesImmutableBatchToListeners() {
    EntityChangeLogMapper mapper = mock(EntityChangeLogMapper.class);
    EntityChangeRecord first = change(1L, "CATALOG", "ml1.cat1");
    EntityChangeRecord second = change(2L, "SCHEMA", "ml1.cat1.sch1");
    when(mapper.selectEntityChanges(0L, 2000)).thenReturn(new ArrayList<>(List.of(first, second)));

    List<EntityChangeRecord> received = new ArrayList<>();

    try (MockedStatic<SessionUtils> sessionUtils = mockStatic(SessionUtils.class)) {
      sessionUtils
          .when(() -> SessionUtils.getWithoutCommit(any(), any()))
          .thenAnswer(
              invocation -> {
                Function<Object, Object> func = invocation.getArgument(1);
                return func.apply(mapper);
              });
      EntityChangeLogPoller poller = new EntityChangeLogPoller(1);
      poller.registerListener(
          changes -> Assertions.assertThrows(UnsupportedOperationException.class, changes::clear));
      poller.registerListener(received::addAll);

      poller.pollChanges();
    }

    Assertions.assertEquals(List.of(first, second), received);
  }

  private static EntityChangeRecord change(long id, String type, String fullName) {
    return new EntityChangeRecord(id, "ml1", type, fullName, OperateType.ALTER, 0L);
  }

  private static List<EntityChangeRecord> changes(long firstId, long lastId) {
    List<EntityChangeRecord> changes = new ArrayList<>();
    for (long id = firstId; id <= lastId; id++) {
      changes.add(change(id, "TABLE", "ml1.cat1.schema1.table" + id));
    }
    return changes;
  }

  private static void mockSessionUtils(
      MockedStatic<SessionUtils> sessionUtils, EntityChangeLogMapper mapper) {
    sessionUtils
        .when(() -> SessionUtils.getWithoutCommit(any(), any()))
        .thenAnswer(
            invocation -> {
              Function<Object, Object> func = invocation.getArgument(1);
              return func.apply(mapper);
            });
  }
}
