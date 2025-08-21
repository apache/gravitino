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
package org.apache.gravitino.storage.relational.session;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class SessionUtilsTest {

  private MockedStatic<SqlSessions> mockSqlSessions;
  private SqlSession mockSession;
  private TestMapper mockMapper;

  interface TestMapper {
    String testMethod();

    void testVoidMethod();
  }

  @BeforeEach
  void setUp() {
    mockSqlSessions = mockStatic(SqlSessions.class);
    mockSession = mock(SqlSession.class);
    mockMapper = mock(TestMapper.class);

    mockSqlSessions.when(() -> SqlSessions.getMapper(TestMapper.class)).thenReturn(mockMapper);
  }

  @AfterEach
  void tearDown() {
    mockSqlSessions.close();
  }

  @Test
  void testDoWithCommit_Success() {
    // Given
    Consumer<TestMapper> consumer = TestMapper::testVoidMethod;

    // When
    assertDoesNotThrow(() -> SessionUtils.doWithCommit(TestMapper.class, consumer));

    // Then
    verify(mockMapper).testVoidMethod();
    mockSqlSessions.verify(SqlSessions::commitAndCloseSqlSession);
    mockSqlSessions.verify(SqlSessions::rollbackAndCloseSqlSession, never());
  }

  @Test
  void testDoWithCommit_ExceptionDuringOperation() {
    // Given
    RuntimeException testException = new RuntimeException("Test exception");
    Consumer<TestMapper> consumer =
        mapper -> {
          throw testException;
        };

    // When & Then
    RuntimeException thrown =
        assertThrows(
            RuntimeException.class, () -> SessionUtils.doWithCommit(TestMapper.class, consumer));
    assertEquals(testException, thrown);

    mockSqlSessions.verify(SqlSessions::rollbackAndCloseSqlSession);
    mockSqlSessions.verify(SqlSessions::commitAndCloseSqlSession, never());
  }

  @Test
  void testDoWithCommit_ExceptionDuringCommit() {
    // Given
    RuntimeException commitException = new RuntimeException("Commit failed");
    Consumer<TestMapper> consumer = TestMapper::testVoidMethod;
    mockSqlSessions.when(SqlSessions::commitAndCloseSqlSession).thenThrow(commitException);

    // When & Then
    RuntimeException thrown =
        assertThrows(
            RuntimeException.class, () -> SessionUtils.doWithCommit(TestMapper.class, consumer));
    assertEquals(commitException, thrown);

    verify(mockMapper).testVoidMethod();
    mockSqlSessions.verify(SqlSessions::rollbackAndCloseSqlSession);
  }

  @Test
  void testDoWithCommitAndFetchResult_Success() {
    // Given
    String expectedResult = "test result";
    when(mockMapper.testMethod()).thenReturn(expectedResult);
    Function<TestMapper, String> function = TestMapper::testMethod;

    // When
    String result = SessionUtils.doWithCommitAndFetchResult(TestMapper.class, function);

    // Then
    assertEquals(expectedResult, result);
    verify(mockMapper).testMethod();
    mockSqlSessions.verify(SqlSessions::commitAndCloseSqlSession);
    mockSqlSessions.verify(SqlSessions::rollbackAndCloseSqlSession, never());
  }

  @Test
  void testDoWithCommitAndFetchResult_ExceptionDuringOperation() {
    // Given
    RuntimeException testException = new RuntimeException("Test exception");
    Function<TestMapper, String> function =
        mapper -> {
          throw testException;
        };

    // When & Then
    RuntimeException thrown =
        assertThrows(
            RuntimeException.class,
            () -> SessionUtils.doWithCommitAndFetchResult(TestMapper.class, function));
    assertEquals(testException, thrown);

    mockSqlSessions.verify(SqlSessions::rollbackAndCloseSqlSession);
    mockSqlSessions.verify(SqlSessions::commitAndCloseSqlSession, never());
  }

  @Test
  void testDoWithoutCommitAndFetchResult_WithExistingSession() {
    // Given
    String expectedResult = "test result";
    when(mockMapper.testMethod()).thenReturn(expectedResult);
    mockSqlSessions.when(SqlSessions::peekSqlSession).thenReturn(mockSession);
    Function<TestMapper, String> function = TestMapper::testMethod;

    // When
    String result = SessionUtils.doWithoutCommitAndFetchResult(TestMapper.class, function);

    // Then
    assertEquals(expectedResult, result);
    verify(mockMapper).testMethod();

    mockSqlSessions.verify(SqlSessions::rollbackAndCloseSqlSession, never());
  }

  @Test
  void testDoWithoutCommitAndFetchResult_WithoutExistingSession() {
    // Given
    String expectedResult = "test result";
    when(mockMapper.testMethod()).thenReturn(expectedResult);
    mockSqlSessions.when(SqlSessions::peekSqlSession).thenReturn(null);
    Function<TestMapper, String> function = TestMapper::testMethod;

    // When
    String result = SessionUtils.doWithoutCommitAndFetchResult(TestMapper.class, function);

    // Then
    assertEquals(expectedResult, result);
    verify(mockMapper).testMethod();

    mockSqlSessions.verify(SqlSessions::rollbackAndCloseSqlSession);
  }

  @Test
  void testDoWithoutCommitAndFetchResult_ExceptionWithNewSession() {
    // Given
    RuntimeException testException = new RuntimeException("Test exception");
    mockSqlSessions.when(SqlSessions::peekSqlSession).thenReturn(null);
    Function<TestMapper, String> function =
        mapper -> {
          throw testException;
        };

    // When & Then
    RuntimeException thrown =
        assertThrows(
            RuntimeException.class,
            () -> SessionUtils.doWithoutCommitAndFetchResult(TestMapper.class, function));
    assertEquals(testException, thrown);

    mockSqlSessions.verify(SqlSessions::rollbackAndCloseSqlSession);
  }

  @Test
  void testDoWithoutCommit_WithExistingSession() {
    // Given
    Consumer<TestMapper> consumer = TestMapper::testVoidMethod;
    mockSqlSessions.when(SqlSessions::peekSqlSession).thenReturn(mockSession);

    // When
    assertDoesNotThrow(() -> SessionUtils.doWithoutCommit(TestMapper.class, consumer));

    // Then
    verify(mockMapper).testVoidMethod();

    mockSqlSessions.verify(SqlSessions::rollbackAndCloseSqlSession, never());
  }

  @Test
  void testDoWithoutCommit_WithoutExistingSession() {
    // Given
    Consumer<TestMapper> consumer = TestMapper::testVoidMethod;
    mockSqlSessions.when(SqlSessions::peekSqlSession).thenReturn(null);

    // When
    assertDoesNotThrow(() -> SessionUtils.doWithoutCommit(TestMapper.class, consumer));

    // Then
    verify(mockMapper).testVoidMethod();

    mockSqlSessions.verify(SqlSessions::rollbackAndCloseSqlSession);
  }

  @Test
  void testGetWithoutCommit_Success() {
    // Given
    String expectedResult = "test result";
    when(mockMapper.testMethod()).thenReturn(expectedResult);
    mockSqlSessions.when(SqlSessions::peekSqlSession).thenReturn(null);
    Function<TestMapper, String> function = TestMapper::testMethod;

    // When
    String result = SessionUtils.getWithoutCommit(TestMapper.class, function);

    // Then
    assertEquals(expectedResult, result);
    verify(mockMapper).testMethod();
    mockSqlSessions.verify(SqlSessions::rollbackAndCloseSqlSession);
  }

  @Test
  void testDoMultipleWithCommit_WithExistingSession() {
    // Given
    mockSqlSessions.when(SqlSessions::peekSqlSession).thenReturn(mockSession);
    Runnable operation1 = mock(Runnable.class);
    Runnable operation2 = mock(Runnable.class);

    // When
    assertDoesNotThrow(() -> SessionUtils.doMultipleWithCommit(operation1, operation2));

    // Then
    verify(operation1).run();
    verify(operation2).run();

    mockSqlSessions.verify(SqlSessions::commitAndCloseSqlSession, never());
    mockSqlSessions.verify(SqlSessions::rollbackAndCloseSqlSession, never());
  }

  @Test
  void testDoMultipleWithCommit_WithoutExistingSession() {
    // Given
    mockSqlSessions.when(SqlSessions::peekSqlSession).thenReturn(null);
    Runnable operation1 = mock(Runnable.class);
    Runnable operation2 = mock(Runnable.class);

    // When
    assertDoesNotThrow(() -> SessionUtils.doMultipleWithCommit(operation1, operation2));

    // Then
    verify(operation1).run();
    verify(operation2).run();
    mockSqlSessions.verify(SqlSessions::getSqlSession);
    mockSqlSessions.verify(SqlSessions::commitAndCloseSqlSession);
    mockSqlSessions.verify(SqlSessions::rollbackAndCloseSqlSession, never());
  }

  @Test
  void testDoMultipleWithCommit_ExceptionDuringOperation() {
    // Given
    RuntimeException testException = new RuntimeException("Test exception");
    mockSqlSessions.when(SqlSessions::peekSqlSession).thenReturn(null);
    Runnable operation1 = mock(Runnable.class);
    Runnable operation2 =
        () -> {
          throw testException;
        };

    // When & Then
    RuntimeException thrown =
        assertThrows(
            RuntimeException.class,
            () -> SessionUtils.doMultipleWithCommit(operation1, operation2));
    assertEquals(testException, thrown);

    verify(operation1).run();
    mockSqlSessions.verify(SqlSessions::getSqlSession);
    mockSqlSessions.verify(SqlSessions::rollbackAndCloseSqlSession);
    mockSqlSessions.verify(SqlSessions::commitAndCloseSqlSession, never());
  }

  @Test
  void testDoMultipleWithCommit_ExceptionDuringCommit() {
    // Given
    RuntimeException commitException = new RuntimeException("Commit failed");
    mockSqlSessions.when(SqlSessions::peekSqlSession).thenReturn(null);
    mockSqlSessions.when(SqlSessions::commitAndCloseSqlSession).thenThrow(commitException);
    Runnable operation1 = mock(Runnable.class);

    // When & Then
    RuntimeException thrown =
        assertThrows(RuntimeException.class, () -> SessionUtils.doMultipleWithCommit(operation1));
    assertEquals(commitException, thrown);

    verify(operation1).run();
    mockSqlSessions.verify(SqlSessions::getSqlSession);
    mockSqlSessions.verify(SqlSessions::rollbackAndCloseSqlSession);
  }

  @Test
  void testSessionCleanupAfterRollbackException() {
    // Given
    RuntimeException mapperException = new RuntimeException("Mapper exception");
    RuntimeException rollbackException = new RuntimeException("Rollback failed");

    mockSqlSessions.when(() -> SqlSessions.getMapper(TestMapper.class)).thenThrow(mapperException);
    mockSqlSessions.when(SqlSessions::rollbackAndCloseSqlSession).thenThrow(rollbackException);

    Consumer<TestMapper> consumer = TestMapper::testVoidMethod;

    // When & Then
    RuntimeException thrown =
        assertThrows(
            RuntimeException.class, () -> SessionUtils.doWithCommit(TestMapper.class, consumer));
    assertEquals(rollbackException, thrown);

    mockSqlSessions.verify(SqlSessions::rollbackAndCloseSqlSession);
  }
}
