/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.gravitino.storage.relational.mapper.EntityChangeLogMapper;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

public class TestEntityChangeLogCleaner {

  private static final long RETENTION_MS = TimeUnit.DAYS.toMillis(30);

  @Test
  void testRejectsInvalidConfiguration() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new EntityChangeLogCleaner(-1, TimeUnit.DAYS.toMillis(1)));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> new EntityChangeLogCleaner(RETENTION_MS, 0));
  }

  @Test
  void testDisablesCleanupWhenRetentionIsZero() {
    EntityChangeLogMapper mapper = mock(EntityChangeLogMapper.class);

    try (MockedStatic<SessionUtils> sessionUtils = mockStatic(SessionUtils.class)) {
      mockSessionUtils(sessionUtils, mapper);
      EntityChangeLogCleaner cleaner = new EntityChangeLogCleaner(0, TimeUnit.DAYS.toMillis(1));

      cleaner.cleanExpiredChanges();
    }

    verify(mapper, never()).pruneOldEntityChanges(0);
  }

  @Test
  void testDrainsExpiredRowsInCommittedBatches() {
    EntityChangeLogMapper mapper = mock(EntityChangeLogMapper.class);
    when(mapper.pruneOldEntityChanges(RETENTION_MS))
        .thenReturn(EntityChangeLogMapper.ENTITY_CHANGE_LOG_PRUNE_BATCH_SIZE)
        .thenReturn(EntityChangeLogMapper.ENTITY_CHANGE_LOG_PRUNE_BATCH_SIZE)
        .thenReturn(7);

    try (MockedStatic<SessionUtils> sessionUtils = mockStatic(SessionUtils.class)) {
      mockSessionUtils(sessionUtils, mapper);
      EntityChangeLogCleaner cleaner =
          new EntityChangeLogCleaner(RETENTION_MS, TimeUnit.DAYS.toMillis(1));

      cleaner.cleanExpiredChanges();
    }

    verify(mapper, times(3)).pruneOldEntityChanges(RETENTION_MS);
  }

  @Test
  void testCleanupFailureDoesNotEscapeScheduledTask() {
    EntityChangeLogMapper mapper = mock(EntityChangeLogMapper.class);
    when(mapper.pruneOldEntityChanges(RETENTION_MS))
        .thenThrow(new RuntimeException("database unavailable"));

    try (MockedStatic<SessionUtils> sessionUtils = mockStatic(SessionUtils.class)) {
      mockSessionUtils(sessionUtils, mapper);
      EntityChangeLogCleaner cleaner =
          new EntityChangeLogCleaner(RETENTION_MS, TimeUnit.DAYS.toMillis(1));

      Assertions.assertDoesNotThrow(cleaner::cleanExpiredChanges);
    }
  }

  private static void mockSessionUtils(
      MockedStatic<SessionUtils> sessionUtils, EntityChangeLogMapper mapper) {
    sessionUtils
        .when(() -> SessionUtils.doWithCommitAndFetchResult(any(), any()))
        .thenAnswer(
            invocation -> {
              Function<Object, Object> function = invocation.getArgument(1);
              return function.apply(mapper);
            });
  }
}
