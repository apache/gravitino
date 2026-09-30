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

package org.apache.gravitino.utils;

import java.security.PrivilegedActionException;
import java.util.ArrayList;
import java.util.List;
import org.apache.gravitino.UserPrincipal;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestPrincipalUtils {

  @Test
  public void testNormal() throws Exception {
    UserPrincipal principal = new UserPrincipal("testNormal");
    PrincipalUtils.doAs(
        principal,
        () -> {
          Assertions.assertEquals("testNormal", PrincipalUtils.getCurrentPrincipal().getName());
          return null;
        });
  }

  @Test
  public void testThread() throws Exception {
    UserPrincipal principal = new UserPrincipal("testThread");
    PrincipalUtils.doAs(
        principal,
        () -> {
          Thread thread =
              new Thread(
                  () ->
                      Assertions.assertEquals(
                          "testThread", PrincipalUtils.getCurrentPrincipal().getName()));
          thread.start();
          thread.join();
          return null;
        });
  }

  @Test
  public void testErrorIsPropagated() {
    UserPrincipal principal = new UserPrincipal("testErrorIsPropagated");
    AssertionError error = new AssertionError("test error");

    AssertionError thrown =
        Assertions.assertThrows(
            AssertionError.class,
            () ->
                PrincipalUtils.doAs(
                    principal,
                    () -> {
                      throw error;
                    }));

    Assertions.assertSame(error, thrown);
  }

  /** Checks that checked exceptions retain their identity and cause. */
  @Test
  public void testCheckedExceptionIsPropagated() {
    Exception cause = new Exception("root cause");
    Exception exception = new Exception("checked failure", cause);
    Exception thrown =
        Assertions.assertThrows(
            Exception.class,
            () ->
                PrincipalUtils.doAs(
                    new UserPrincipal("test"),
                    () -> {
                      throw exception;
                    }));
    Assertions.assertSame(exception, thrown);
    Assertions.assertSame(cause, thrown.getCause());
  }

  /** Checks that runtime exceptions retain their identity and cause. */
  @Test
  public void testRuntimeExceptionIsPropagated() {
    Exception cause = new Exception("root cause");
    RuntimeException exception = new IllegalArgumentException("invalid argument", cause);
    RuntimeException thrown =
        Assertions.assertThrows(
            RuntimeException.class,
            () ->
                PrincipalUtils.doAs(
                    new UserPrincipal("test"),
                    () -> {
                      throw exception;
                    }));
    Assertions.assertSame(exception, thrown);
    Assertions.assertSame(cause, thrown.getCause());
  }

  /** Checks that caught failures are logged at ERROR with the throwable. */
  @Test
  public void testFailuresAreLoggedWithThrowable() {
    LoggerContext context =
        (LoggerContext) LogManager.getContext(PrincipalUtils.class.getClassLoader(), false);
    Configuration configuration = context.getConfiguration();
    String loggerName = PrincipalUtils.class.getName();
    LoggerConfig previousConfig = configuration.getLoggers().get(loggerName);
    Appender appender = Mockito.mock(Appender.class);
    Mockito.when(appender.getName()).thenReturn("principalUtilsCapture");
    Mockito.when(appender.isStarted()).thenReturn(true);
    List<LogEvent> events = new ArrayList<>();
    Mockito.doAnswer(
            invocation -> {
              events.add(((LogEvent) invocation.getArgument(0)).toImmutable());
              return null;
            })
        .when(appender)
        .append(Mockito.any(LogEvent.class));
    LoggerConfig loggerConfig = new LoggerConfig(loggerName, Level.ERROR, false);
    loggerConfig.addAppender(appender, Level.ERROR, null);
    configuration.removeLogger(loggerName);
    configuration.addLogger(loggerName, loggerConfig);
    context.updateLoggers();
    try {
      Error error = new AssertionError("request error");
      Assertions.assertThrows(
          Error.class,
          () ->
              PrincipalUtils.doAs(
                  new UserPrincipal("test"),
                  () -> {
                    throw error;
                  }));
      Exception exception = new Exception("checked failure", new Exception("root cause"));
      Assertions.assertThrows(
          Exception.class,
          () ->
              PrincipalUtils.doAs(
                  new UserPrincipal("test"),
                  () -> {
                    throw exception;
                  }));
      Assertions.assertEquals(2, events.size());
      Assertions.assertEquals(Level.ERROR, events.get(0).getLevel());
      Assertions.assertSame(error, events.get(0).getThrown());
      Assertions.assertEquals(Level.ERROR, events.get(1).getLevel());
      Throwable logged = events.get(1).getThrown();
      Assertions.assertInstanceOf(PrivilegedActionException.class, logged);
      Assertions.assertSame(exception, logged.getCause());
    } finally {
      configuration.removeLogger(loggerName);
      if (previousConfig != null) {
        configuration.addLogger(loggerName, previousConfig);
      }
      context.updateLoggers();
    }
  }
}
