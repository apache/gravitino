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

package org.apache.gravitino.storage.relational.utils;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;

import org.apache.gravitino.storage.relational.session.SqlSessions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

public class TestSessionUtils {

  @Test
  public void testDoWithCommitShouldRollbackOnAssertionError() {
    Object mapper = new Object();

    try (MockedStatic<SqlSessions> mockedSqlSessions = mockStatic(SqlSessions.class)) {
      mockedSqlSessions.when(() -> SqlSessions.getWriteMapper(Object.class)).thenReturn(mapper);

      assertThrows(
          AssertionError.class,
          () ->
              SessionUtils.doWithCommit(
                  Object.class,
                  ignored -> {
                    throw new AssertionError("boom");
                  }));

      mockedSqlSessions.verify(SqlSessions::rollbackAndCloseSqlSession);
      mockedSqlSessions.verify(() -> SqlSessions.commitAndCloseSqlSession(), never());
    }
  }

  @Test
  public void testDoWithCommitAndFetchResultShouldRollbackOnAssertionError() {
    Object mapper = new Object();

    try (MockedStatic<SqlSessions> mockedSqlSessions = mockStatic(SqlSessions.class)) {
      mockedSqlSessions.when(() -> SqlSessions.getWriteMapper(Object.class)).thenReturn(mapper);

      assertThrows(
          AssertionError.class,
          () ->
              SessionUtils.doWithCommitAndFetchResult(
                  Object.class,
                  ignored -> {
                    throw new AssertionError("boom");
                  }));

      mockedSqlSessions.verify(SqlSessions::rollbackAndCloseSqlSession);
      mockedSqlSessions.verify(() -> SqlSessions.commitAndCloseSqlSession(), never());
    }
  }

  @Test
  public void testDoMultipleWithCommitShouldRollbackOnAssertionError() {
    try (MockedStatic<SqlSessions> mockedSqlSessions = mockStatic(SqlSessions.class)) {
      assertThrows(
          AssertionError.class,
          () ->
              SessionUtils.doMultipleWithCommit(
                  () -> {
                    throw new AssertionError("boom");
                  }));

      mockedSqlSessions.verify(SqlSessions::getWriteSqlSession);
      mockedSqlSessions.verify(SqlSessions::rollbackAndCloseSqlSession);
      mockedSqlSessions.verify(() -> SqlSessions.commitAndCloseSqlSession(), never());
    }
  }
}
