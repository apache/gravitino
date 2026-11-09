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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeout;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.channels.spi.AbstractInterruptibleChannel;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.SQLTransientConnectionException;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.security.auth.Subject;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class TestBoundedConnectionDriver {
  private final ExecutorService workers = Executors.newSingleThreadExecutor();

  @AfterEach
  void close() throws Exception {
    workers.shutdownNow();
    assertTrue(workers.awaitTermination(10, TimeUnit.SECONDS));
  }

  @Test
  void testDeadlineDoesNotRunBlockingInterruptCallbackOnCaller() throws Exception {
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch releaseDriver = new CountDownLatch(1);
    CountDownLatch callbackEntered = new CountDownLatch(1);
    CountDownLatch releaseCallback = new CountDownLatch(1);
    AtomicReference<Thread> callbackThread = new AtomicReference<>();
    class BlockingInterruptChannel extends AbstractInterruptibleChannel {
      void register() {
        begin();
      }

      void finish() throws IOException {
        end(false);
      }
      /** {@inheritDoc} */
      @Override
      protected void implCloseChannel() {
        callbackThread.set(Thread.currentThread());
        callbackEntered.countDown();
        awaitIgnoringInterrupts(releaseCallback);
      }
    }
    BlockingInterruptChannel channel = new BlockingInterruptChannel();
    Driver driver = mock(Driver.class);
    Connection late = mock(Connection.class);
    when(driver.connect(anyString(), any()))
        .thenAnswer(
            call -> {
              channel.register();
              entered.countDown();
              try {
                awaitIgnoringInterrupts(releaseDriver);
              } finally {
                channel.finish();
              }
              return late;
            });
    BoundedConnectionDriver bounded = new BoundedConnectionDriver(driver, 500, workers);
    ExecutorService callers = Executors.newSingleThreadExecutor();
    AtomicReference<Thread> callerThread = new AtomicReference<>();
    try {
      Future<SQLTimeoutException> result =
          callers.submit(
              () -> {
                callerThread.set(Thread.currentThread());
                return assertThrows(
                    SQLTimeoutException.class,
                    () -> bounded.connect("jdbc:test", new Properties()));
              });
      assertTrue(entered.await(10, TimeUnit.SECONDS));
      // The caller returns at its deadline while the interrupt callback is still blocked.
      assertNotNull(result.get(2, TimeUnit.SECONDS));
      assertTrue(callbackEntered.await(10, TimeUnit.SECONDS), "The worker must be interrupted");
      assertNotSame(callerThread.get(), callbackThread.get());
      assertTrue(callbackThread.get().getName().startsWith("jdbc-connect-cancel"));
      assertThrows(
          SQLTransientConnectionException.class,
          () -> bounded.connect("jdbc:test", new Properties()));
      verify(late, never()).close();
    } finally {
      releaseCallback.countDown();
      releaseDriver.countDown();
      callers.shutdownNow();
      assertTrue(callers.awaitTermination(10, TimeUnit.SECONDS));
    }
    // The interrupt closed the driver's channel, so its connect fails instead of returning late.
    workers.submit(() -> {}).get(10, TimeUnit.SECONDS);
    verify(late, never()).close();
    doReturn(late).when(driver).connect(anyString(), any());
    assertSame(late, bounded.connect("jdbc:test", new Properties()));
  }

  @Test
  void testTimeoutInterruptsWorkerSoItIsReleased() throws Exception {
    Driver driver = mock(Driver.class);
    Connection connection = mock(Connection.class);
    CountDownLatch never = new CountDownLatch(1);
    CountDownLatch interrupted = new CountDownLatch(1);
    when(driver.connect(anyString(), any()))
        .thenAnswer(
            call -> {
              try {
                never.await(30, TimeUnit.SECONDS);
                throw new AssertionError("The hung connect was not interrupted");
              } catch (InterruptedException e) {
                interrupted.countDown();
                throw new SQLException("Interrupted", "08001", e);
              }
            })
        .thenReturn(connection);
    BoundedConnectionDriver bounded = new BoundedConnectionDriver(driver, 100, workers);
    assertThrows(SQLTimeoutException.class, () -> bounded.connect("jdbc:test", new Properties()));
    assertTrue(interrupted.await(10, TimeUnit.SECONDS));
    // The single worker is free again and the quarantine is lifted.
    workers.submit(() -> {}).get(10, TimeUnit.SECONDS);
    assertSame(connection, bounded.connect("jdbc:test", new Properties()));
  }

  @Test
  void testLateCancellationDoesNotInterruptNextAttemptOnSameWorker() throws Exception {
    Driver driver = mock(Driver.class);
    Connection first = mock(Connection.class);
    Connection second = mock(Connection.class);
    CountDownLatch releaseFirst = new CountDownLatch(1);
    CountDownLatch secondEntered = new CountDownLatch(1);
    CountDownLatch releaseSecond = new CountDownLatch(1);
    AtomicReference<Boolean> secondInterrupted = new AtomicReference<>(false);
    when(driver.connect(anyString(), any()))
        .thenAnswer(
            call -> {
              awaitIgnoringInterrupts(releaseFirst);
              return first;
            })
        .thenAnswer(
            call -> {
              secondEntered.countDown();
              try {
                releaseSecond.await(10, TimeUnit.SECONDS);
              } catch (InterruptedException e) {
                secondInterrupted.set(true);
              }
              return second;
            });
    // Hold the cancellation until the abandoned attempt has finished and the worker moved on.
    AtomicReference<Runnable> cancellation = new AtomicReference<>();
    ExecutorService manual = mock(ExecutorService.class);
    doAnswer(
            call -> {
              cancellation.set(call.getArgument(0));
              return null;
            })
        .when(manual)
        .execute(any());
    BoundedConnectionDriver bounded = new BoundedConnectionDriver(driver, 100, workers, manual);
    assertThrows(SQLTimeoutException.class, () -> bounded.connect("jdbc:test", new Properties()));
    releaseFirst.countDown();
    workers.submit(() -> {}).get(10, TimeUnit.SECONDS);
    verify(first).close();

    ExecutorService callers = Executors.newSingleThreadExecutor();
    try {
      Future<Connection> next =
          callers.submit(() -> bounded.connect("jdbc:test", new Properties()));
      assertTrue(secondEntered.await(10, TimeUnit.SECONDS));
      assertNotNull(cancellation.get());
      cancellation.get().run();
      releaseSecond.countDown();
      assertSame(second, next.get(10, TimeUnit.SECONDS));
      assertFalse(secondInterrupted.get(), "A stale cancellation must not reach a later attempt");
    } finally {
      releaseSecond.countDown();
      callers.shutdownNow();
      assertTrue(callers.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  /**
   * The canceller has decided to interrupt and is inside {@link Thread#interrupt} when the connect
   * returns. The worker must wait for that interrupt instead of moving on, otherwise it would land
   * on the next attempt that the same worker runs.
   */
  @Test
  void testInFlightCancellationIsDeliveredBeforeWorkerMovesOn() throws Exception {
    CountDownLatch interruptEntered = new CountDownLatch(1);
    CountDownLatch releaseInterrupt = new CountDownLatch(1);
    ExecutorService worker =
        Executors.newSingleThreadExecutor(
            task ->
                new Thread(task) {
                  /** {@inheritDoc} */
                  @Override
                  public void interrupt() {
                    if (Thread.currentThread() != this) {
                      interruptEntered.countDown();
                      awaitIgnoringInterrupts(releaseInterrupt);
                    }
                    super.interrupt();
                  }
                });
    Driver driver = mock(Driver.class);
    Connection first = mock(Connection.class);
    Connection second = mock(Connection.class);
    CountDownLatch releaseFirst = new CountDownLatch(1);
    CountDownLatch secondEntered = new CountDownLatch(1);
    CountDownLatch releaseSecond = new CountDownLatch(1);
    AtomicReference<Boolean> secondInterrupted = new AtomicReference<>(false);
    when(driver.connect(anyString(), any()))
        .thenAnswer(
            call -> {
              awaitIgnoringInterrupts(releaseFirst);
              return first;
            })
        .thenAnswer(
            call -> {
              secondEntered.countDown();
              try {
                releaseSecond.await(10, TimeUnit.SECONDS);
              } catch (InterruptedException e) {
                secondInterrupted.set(true);
              }
              return second;
            });
    AtomicReference<Runnable> cancellation = new AtomicReference<>();
    ExecutorService manual = mock(ExecutorService.class);
    doAnswer(
            call -> {
              cancellation.set(call.getArgument(0));
              return null;
            })
        .when(manual)
        .execute(any());
    // A second datasource on the same shared workers, so its attempt is not quarantined.
    BoundedConnectionDriver timedOut = new BoundedConnectionDriver(driver, 100, worker, manual);
    BoundedConnectionDriver healthy = new BoundedConnectionDriver(driver, 10000, worker, manual);
    ExecutorService callers = Executors.newSingleThreadExecutor();
    Thread canceller = null;
    try {
      assertThrows(
          SQLTimeoutException.class, () -> timedOut.connect("jdbc:test", new Properties()));
      canceller = new Thread(cancellation.get());
      canceller.start();
      assertTrue(interruptEntered.await(10, TimeUnit.SECONDS));
      releaseFirst.countDown();
      Future<Connection> next =
          callers.submit(() -> healthy.connect("jdbc:test", new Properties()));
      assertFalse(
          secondEntered.await(300, TimeUnit.MILLISECONDS),
          "The worker must not start another attempt while an interrupt is being delivered");
      releaseInterrupt.countDown();
      canceller.join(10000);
      assertTrue(secondEntered.await(10, TimeUnit.SECONDS));
      releaseSecond.countDown();
      assertSame(second, next.get(10, TimeUnit.SECONDS));
      assertFalse(secondInterrupted.get(), "The cancellation must not reach the next attempt");
      verify(first).close();
    } finally {
      releaseFirst.countDown();
      releaseInterrupt.countDown();
      releaseSecond.countDown();
      if (canceller != null) {
        canceller.join(10000);
      }
      callers.shutdownNow();
      worker.shutdownNow();
      assertTrue(callers.awaitTermination(10, TimeUnit.SECONDS));
      assertTrue(worker.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  @Test
  void testCancellationAfterConnectReturnsDoesNotInterruptCleanup() throws Exception {
    Driver driver = mock(Driver.class);
    Connection late = mock(Connection.class);
    CountDownLatch release = new CountDownLatch(1);
    CountDownLatch closeEntered = new CountDownLatch(1);
    CountDownLatch releaseClose = new CountDownLatch(1);
    AtomicReference<Boolean> closeInterrupted = new AtomicReference<>();
    when(driver.connect(anyString(), any()))
        .thenAnswer(
            call -> {
              awaitIgnoringInterrupts(release);
              return late;
            });
    doAnswer(
            call -> {
              closeEntered.countDown();
              awaitIgnoringInterrupts(releaseClose);
              closeInterrupted.set(Thread.currentThread().isInterrupted());
              return null;
            })
        .when(late)
        .close();
    AtomicReference<Runnable> cancellation = new AtomicReference<>();
    ExecutorService manual = mock(ExecutorService.class);
    doAnswer(
            call -> {
              cancellation.set(call.getArgument(0));
              return null;
            })
        .when(manual)
        .execute(any());
    BoundedConnectionDriver bounded = new BoundedConnectionDriver(driver, 100, workers, manual);
    try {
      assertThrows(SQLTimeoutException.class, () -> bounded.connect("jdbc:test", new Properties()));
      release.countDown();
      assertTrue(closeEntered.await(10, TimeUnit.SECONDS));
      // The connect has returned; cancelling now must not interrupt closing the late connection.
      cancellation.get().run();
    } finally {
      release.countDown();
      releaseClose.countDown();
    }
    workers.submit(() -> {}).get(10, TimeUnit.SECONDS);
    assertEquals(false, closeInterrupted.get());
  }

  @Test
  void testSuccessfulHandoffAndContextRestoration() throws Exception {
    Driver driver = mock(Driver.class);
    Connection connection = mock(Connection.class);
    Properties properties = new Properties();
    properties.setProperty("user", "test");
    ClassLoader workerLoader =
        workers.submit(() -> Thread.currentThread().getContextClassLoader()).get();
    ClassLoader callerLoader = new ClassLoader(workerLoader) {};
    AtomicReference<Properties> received = new AtomicReference<>();
    when(driver.connect(eq("jdbc:test"), any()))
        .thenAnswer(
            invocation -> {
              assertSame(callerLoader, Thread.currentThread().getContextClassLoader());
              received.set(invocation.getArgument(1));
              return connection;
            });
    ClassLoader previous = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(callerLoader);
      BoundedConnectionDriver bounded = new BoundedConnectionDriver(driver, 5000, workers);
      assertSame(connection, bounded.connect("jdbc:test", properties));
    } finally {
      Thread.currentThread().setContextClassLoader(previous);
    }
    assertEquals(properties, received.get());
    assertNotSame(properties, received.get());
    assertSame(
        workerLoader, workers.submit(() -> Thread.currentThread().getContextClassLoader()).get());
    verify(connection, never()).close();
  }

  @Test
  @SuppressWarnings("removal")
  void testConnectPreservesCallerSubjectOnExistingWorker() throws Exception {
    workers.submit(() -> {}).get(10, TimeUnit.SECONDS);
    Driver driver = mock(Driver.class);
    Connection connection = mock(Connection.class);
    AtomicReference<Subject> observed = new AtomicReference<>();
    when(driver.connect(anyString(), any()))
        .thenAnswer(
            invocation -> {
              observed.set(Subject.getSubject(AccessController.getContext()));
              return connection;
            });
    BoundedConnectionDriver bounded = new BoundedConnectionDriver(driver, 5000, workers);
    Subject subject = new Subject();

    assertSame(
        connection,
        Subject.doAs(
            subject,
            (PrivilegedExceptionAction<Connection>)
                () -> bounded.connect("jdbc:test", new Properties())));
    assertSame(subject, observed.get());
    workers.submit(() -> {}).get(10, TimeUnit.SECONDS);
    assertNull(workers.submit(() -> Subject.getSubject(AccessController.getContext())).get());
  }

  @Test
  @SuppressWarnings("removal")
  void testReusedWorkerDoesNotAuthenticateAsPreviousCaller() throws Exception {
    Driver driver = mock(Driver.class);
    AtomicReference<Subject> observed = new AtomicReference<>();
    when(driver.connect(anyString(), any()))
        .thenAnswer(
            invocation -> {
              observed.set(Subject.getSubject(AccessController.getContext()));
              return null;
            });
    BoundedConnectionDriver bounded = new BoundedConnectionDriver(driver, 5000, workers);
    Subject first = new Subject();
    Subject second = new Subject();
    Subject.doAs(
        first,
        (PrivilegedExceptionAction<Connection>)
            () -> bounded.connect("jdbc:test", new Properties()));
    assertSame(first, observed.get());
    workers.submit(() -> {}).get(10, TimeUnit.SECONDS);
    Subject.doAs(
        second,
        (PrivilegedExceptionAction<Connection>)
            () -> bounded.connect("jdbc:test", new Properties()));
    assertSame(second, observed.get());
    workers.submit(() -> {}).get(10, TimeUnit.SECONDS);
    bounded.connect("jdbc:test", new Properties());
    assertNull(observed.get());
  }

  @Test
  @SuppressWarnings("removal")
  void testSharedWorkerDoesNotInheritCreatingCallerSubject() throws Exception {
    ThreadPoolExecutor pool =
        (ThreadPoolExecutor)
            FieldUtils.readStaticField(BoundedConnectionDriver.class, "WORKERS", true);
    AtomicReference<Subject> observed = new AtomicReference<>();
    Subject subject = new Subject();
    Thread worker =
        Subject.doAs(
            subject,
            (PrivilegedExceptionAction<Thread>)
                () ->
                    pool.getThreadFactory()
                        .newThread(
                            () -> observed.set(Subject.getSubject(AccessController.getContext()))));
    worker.start();
    worker.join(10000);
    assertFalse(worker.isAlive());
    assertNull(observed.get());
  }

  @Test
  @SuppressWarnings("removal")
  void testLateClosePreservesSubmittingSubject() throws Exception {
    workers.submit(() -> {}).get(10, TimeUnit.SECONDS);
    Driver driver = mock(Driver.class);
    Connection late = mock(Connection.class);
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    AtomicReference<Subject> observed = new AtomicReference<>();
    when(driver.connect(anyString(), any()))
        .thenAnswer(
            invocation -> {
              entered.countDown();
              awaitIgnoringInterrupts(release);
              return late;
            });
    doAnswer(
            invocation -> {
              observed.set(Subject.getSubject(AccessController.getContext()));
              return null;
            })
        .when(late)
        .close();
    BoundedConnectionDriver bounded = new BoundedConnectionDriver(driver, 100, workers);
    Subject subject = new Subject();
    try {
      Subject.doAs(
          subject,
          (PrivilegedExceptionAction<Void>)
              () -> {
                assertThrows(
                    SQLTimeoutException.class,
                    () -> bounded.connect("jdbc:test", new Properties()));
                return null;
              });
      assertTrue(entered.await(10, TimeUnit.SECONDS));
    } finally {
      release.countDown();
    }
    workers.submit(() -> {}).get(10, TimeUnit.SECONDS);
    verify(late).close();
    assertSame(subject, observed.get());
    assertNull(workers.submit(() -> Subject.getSubject(AccessController.getContext())).get());
  }

  @Test
  void testNullPropertiesArePassedAsEmptyProperties() throws Exception {
    Driver driver = mock(Driver.class);
    Connection connection = mock(Connection.class);
    AtomicReference<Properties> received = new AtomicReference<>();
    when(driver.connect(anyString(), any()))
        .thenAnswer(
            invocation -> {
              received.set(invocation.getArgument(1));
              return connection;
            });
    BoundedConnectionDriver bounded = new BoundedConnectionDriver(driver, 5000, workers);
    assertSame(connection, bounded.connect("jdbc:test", null));
    assertNotNull(received.get());
    assertTrue(received.get().isEmpty());
  }

  @Test
  void testLateCloseRunsWithoutCancellationInterrupt() throws Exception {
    Driver driver = mock(Driver.class);
    Connection late = mock(Connection.class);
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    AtomicReference<Boolean> closed = new AtomicReference<>(false);
    when(driver.connect(anyString(), any()))
        .thenAnswer(
            invocation -> {
              entered.countDown();
              awaitIgnoringInterrupts(release);
              return late;
            });
    doAnswer(
            invocation -> {
              if (Thread.currentThread().isInterrupted()) {
                throw new SQLException("Cleanup interrupted before closing the connection");
              }
              closed.set(true);
              return null;
            })
        .when(late)
        .close();
    BoundedConnectionDriver bounded = new BoundedConnectionDriver(driver, 100, workers);
    try {
      assertThrows(SQLTimeoutException.class, () -> bounded.connect("jdbc:test", new Properties()));
      assertTrue(entered.await(10, TimeUnit.SECONDS));
    } finally {
      release.countDown();
    }
    workers.submit(() -> {}).get(10, TimeUnit.SECONDS);
    assertTrue(closed.get(), "The physical connection must close despite cancellation");
  }

  @Test
  void testSqlFailurePreservesCauseAndState() throws Exception {
    Driver driver = mock(Driver.class);
    SQLException failure = new SQLException("backend refused", "28000", 17);
    when(driver.connect(anyString(), any())).thenThrow(failure);
    BoundedConnectionDriver bounded = new BoundedConnectionDriver(driver, 5000, workers);
    assertSame(
        failure,
        assertThrows(SQLException.class, () -> bounded.connect("jdbc:test", new Properties())));
  }

  @Test
  void testUnsupportedUrlReturnsNull() throws Exception {
    Driver driver = mock(Driver.class);
    BoundedConnectionDriver bounded = new BoundedConnectionDriver(driver, 5000, workers);
    assertNull(bounded.connect("jdbc:unsupported", new Properties()));
  }

  @Test
  void testTimeoutClosesLateConnectionAndQuarantinesUntilCleanup() throws Exception {
    Driver driver = mock(Driver.class);
    Connection late = mock(Connection.class);
    Connection healthy = mock(Connection.class);
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    CountDownLatch closed = new CountDownLatch(1);
    when(driver.connect(anyString(), any()))
        .thenAnswer(
            invocation -> {
              entered.countDown();
              awaitIgnoringInterrupts(release);
              return late;
            })
        .thenReturn(healthy);
    doAnswer(
            invocation -> {
              closed.countDown();
              return null;
            })
        .when(late)
        .close();
    BoundedConnectionDriver bounded = new BoundedConnectionDriver(driver, 100, workers);
    try {
      SQLTimeoutException error =
          assertTimeout(
              Duration.ofSeconds(3),
              () ->
                  assertThrows(
                      SQLTimeoutException.class,
                      () -> bounded.connect("jdbc:test", new Properties())));
      assertTrue(entered.await(10, TimeUnit.SECONDS));
      assertTrue(error.getMessage().contains("100 ms"));
      assertEquals("08001", error.getSQLState());
      assertNotNull(error.getCause());
      assertThrows(
          SQLTransientConnectionException.class,
          () -> bounded.connect("jdbc:test", new Properties()));
      verify(driver, times(1)).connect(anyString(), any());
      verify(late, never()).close();
    } finally {
      release.countDown();
    }
    assertTrue(closed.await(10, TimeUnit.SECONDS));
    workers.submit(() -> {}).get(10, TimeUnit.SECONDS);
    assertSame(healthy, bounded.connect("jdbc:test", new Properties()));
    verify(late, times(1)).close();
    verify(late, never()).createStatement();
  }

  @Test
  void testBlockedLateCloseKeepsQuarantineUntilCleanupCompletes() throws Exception {
    Driver driver = mock(Driver.class);
    Connection late = mock(Connection.class);
    Connection healthy = mock(Connection.class);
    CountDownLatch connectEntered = new CountDownLatch(1);
    CountDownLatch releaseConnect = new CountDownLatch(1);
    CountDownLatch closeEntered = new CountDownLatch(1);
    CountDownLatch releaseClose = new CountDownLatch(1);
    when(driver.connect(anyString(), any()))
        .thenAnswer(
            invocation -> {
              connectEntered.countDown();
              awaitIgnoringInterrupts(releaseConnect);
              return late;
            })
        .thenReturn(healthy);
    doAnswer(
            invocation -> {
              closeEntered.countDown();
              awaitIgnoringInterrupts(releaseClose);
              return null;
            })
        .when(late)
        .close();
    BoundedConnectionDriver bounded = new BoundedConnectionDriver(driver, 100, workers);
    try {
      assertThrows(SQLTimeoutException.class, () -> bounded.connect("jdbc:test", new Properties()));
      assertTrue(connectEntered.await(10, TimeUnit.SECONDS));
      releaseConnect.countDown();
      assertTrue(closeEntered.await(10, TimeUnit.SECONDS));
      SQLTransientConnectionException error =
          assertThrows(
              SQLTransientConnectionException.class,
              () -> bounded.connect("jdbc:test", new Properties()));
      assertEquals("08001", error.getSQLState());
      verify(driver, times(1)).connect(anyString(), any());
    } finally {
      releaseConnect.countDown();
      releaseClose.countDown();
    }
    workers.submit(() -> {}).get(10, TimeUnit.SECONDS);
    assertSame(healthy, bounded.connect("jdbc:test", new Properties()));
    verify(late, times(1)).close();
  }

  @Test
  void testCallerInterruptClosesLateConnection() throws Exception {
    Driver driver = mock(Driver.class);
    Connection late = mock(Connection.class);
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    CountDownLatch closed = new CountDownLatch(1);
    when(driver.connect(anyString(), any()))
        .thenAnswer(
            invocation -> {
              entered.countDown();
              awaitIgnoringInterrupts(release);
              return late;
            });
    doAnswer(
            invocation -> {
              closed.countDown();
              return null;
            })
        .when(late)
        .close();
    BoundedConnectionDriver bounded = new BoundedConnectionDriver(driver, 30000, workers);
    AtomicReference<Throwable> failure = new AtomicReference<>();
    Thread caller =
        new Thread(
            () -> {
              try {
                assertThrows(
                    SQLTimeoutException.class,
                    () -> bounded.connect("jdbc:test", new Properties()));
                assertTrue(Thread.currentThread().isInterrupted());
              } catch (Throwable error) {
                failure.set(error);
              }
            });
    caller.start();
    try {
      assertTrue(entered.await(10, TimeUnit.SECONDS));
      caller.interrupt();
      caller.join(10000);
      assertFalse(caller.isAlive());
      assertNull(failure.get());
    } finally {
      release.countDown();
      caller.interrupt();
      caller.join(10000);
    }
    assertTrue(closed.await(10, TimeUnit.SECONDS));
    verify(late, times(1)).close();
  }

  @Test
  void testTimeoutBeforeWorkerStartsDoesNotOpenLateConnection() throws Exception {
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    Future<?> blocking =
        workers.submit(
            () -> {
              entered.countDown();
              awaitIgnoringInterrupts(release);
            });
    assertTrue(entered.await(10, TimeUnit.SECONDS));
    Driver driver = mock(Driver.class);
    BoundedConnectionDriver bounded = new BoundedConnectionDriver(driver, 100, workers);
    try {
      assertThrows(SQLTimeoutException.class, () -> bounded.connect("jdbc:test", new Properties()));
      verifyNoInteractions(driver);
    } finally {
      release.countDown();
    }
    blocking.get(10, TimeUnit.SECONDS);
    workers.submit(() -> {}).get(10, TimeUnit.SECONDS);
    assertNull(bounded.connect("jdbc:unsupported", new Properties()));
    verify(driver, times(1)).connect(anyString(), any());
  }

  @Test
  void testSaturationRejectsWithoutCallingDriver() throws Exception {
    ExecutorService single =
        new ThreadPoolExecutor(0, 1, 1, TimeUnit.SECONDS, new SynchronousQueue<>());
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    single.submit(
        () -> {
          entered.countDown();
          awaitIgnoringInterrupts(release);
        });
    assertTrue(entered.await(10, TimeUnit.SECONDS));
    Driver driver = mock(Driver.class);
    try {
      BoundedConnectionDriver bounded = new BoundedConnectionDriver(driver, 1000, single);
      SQLTransientConnectionException error =
          assertThrows(
              SQLTransientConnectionException.class,
              () -> bounded.connect("jdbc:test", new Properties()));
      assertInstanceOf(RejectedExecutionException.class, error.getCause());
      verifyNoInteractions(driver);
    } finally {
      release.countDown();
      single.shutdownNow();
      assertTrue(single.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  @Test
  void testLateCloseFailureDoesNotPinContextOrQuarantine() throws Exception {
    Driver driver = mock(Driver.class);
    Connection late = mock(Connection.class);
    CountDownLatch release = new CountDownLatch(1);
    when(driver.connect(anyString(), any()))
        .thenAnswer(
            invocation -> {
              awaitIgnoringInterrupts(release);
              return late;
            })
        .thenReturn(null);
    doThrow(new SQLException("close failed")).when(late).close();
    ClassLoader loader = workers.submit(() -> Thread.currentThread().getContextClassLoader()).get();
    BoundedConnectionDriver bounded = new BoundedConnectionDriver(driver, 100, workers);
    try {
      assertThrows(SQLTimeoutException.class, () -> bounded.connect("jdbc:test", new Properties()));
    } finally {
      release.countDown();
    }
    assertSame(
        loader,
        workers
            .submit(() -> Thread.currentThread().getContextClassLoader())
            .get(10, TimeUnit.SECONDS));
    assertNull(bounded.connect("jdbc:test", new Properties()));
    verify(late).close();
  }

  private static void awaitIgnoringInterrupts(CountDownLatch latch) {
    boolean interrupted = false;
    while (true) {
      try {
        if (!latch.await(10, TimeUnit.SECONDS)) {
          throw new AssertionError("Test did not release driver");
        }
        break;
      } catch (InterruptedException e) {
        interrupted = true;
      }
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
  }
}
