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

import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.ProtectionDomain;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLTimeoutException;
import java.sql.SQLTransientConnectionException;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bounds physical connection creation for drivers that cannot enforce a login timeout.
 *
 * <p>Only {@link Driver#connect} runs on a worker. Validation, metadata queries and DDL stay on the
 * caller. A connection that arrives after timeout or interruption is closed by its worker and is
 * never handed to a pool. An abandoned attempt's worker is interrupted from a separate cancellation
 * thread, never from the request thread, because an interrupt can synchronously run a driver's
 * blocking channel-close callback. The interrupt is delivered only while the worker is still inside
 * {@link Driver#connect}, so it cannot reach a later attempt on the same worker. A driver that
 * ignores interruption retains its worker until it returns. A JVM-wide limit of 64 workers, with no
 * queue, bounds that retention; excess attempts fail with {@link SQLTransientConnectionException}.
 * Abandoned attempts, their completion and rejected attempts are logged. Workers are daemon threads
 * and release their catalog context class loader after each attempt. Each attempt, including late
 * connection cleanup, runs with the submitting caller's access-control context and JAAS subject;
 * shared workers do not inherit the creating request's JAAS subject. This does not isolate
 * arbitrary {@link ThreadLocal} or {@link InheritableThreadLocal} state. Delegates must clean up
 * such state and must not rely on it to carry a caller's identity. Drivers that cannot meet this
 * requirement must not use this adapter's shared workers.
 *
 * <p>After a timeout, further attempts through this instance fail immediately until the abandoned
 * attempt finishes cleanup. This prevents a lazy pool's initialization lock from serializing a
 * backlog of new attempts against the same unreachable backend. Normal pool waits and SQL execution
 * have their own budgets and are not bounded by this adapter.
 */
@SuppressWarnings("removal") // JDK 17 associates JAAS subjects with access-control contexts.
public final class BoundedConnectionDriver implements Driver {
  private static final Logger LOG = LoggerFactory.getLogger(BoundedConnectionDriver.class);

  private static final AccessControlContext WORKER_CONTEXT =
      new AccessControlContext(new ProtectionDomain[0]);

  private static final ExecutorService WORKERS = daemonPool(64, "jdbc-connect-timeout");

  // One cancellation per abandoned attempt; with the shared workers this stays within their limit.
  // If a cancellation is still rejected, the worker is not interrupted and a warning is logged.
  private static final ExecutorService CANCELLERS = daemonPool(64, "jdbc-connect-cancel");

  private static final long REJECTION_LOG_INTERVAL_NANOS = TimeUnit.MINUTES.toNanos(1);
  private static final AtomicLong LAST_REJECTION_LOG =
      new AtomicLong(System.nanoTime() - REJECTION_LOG_INTERVAL_NANOS);
  private static final AtomicLong REJECTIONS = new AtomicLong();

  private final Driver delegate;
  private final long timeoutMs;
  private final ExecutorService workers;
  private final ExecutorService cancellers;
  private final AtomicInteger abandonedAttempts = new AtomicInteger();

  /**
   * Wraps a driver with a physical connection deadline.
   *
   * @param delegate the driver to call, without registering the wrapper with DriverManager
   * @param timeoutMs a positive timeout in milliseconds
   * @throws IllegalArgumentException if timeoutMs is not positive
   */
  public BoundedConnectionDriver(Driver delegate, long timeoutMs) {
    this(delegate, timeoutMs, WORKERS);
  }

  BoundedConnectionDriver(Driver delegate, long timeoutMs, ExecutorService workers) {
    this(delegate, timeoutMs, workers, CANCELLERS);
  }

  BoundedConnectionDriver(
      Driver delegate, long timeoutMs, ExecutorService workers, ExecutorService cancellers) {
    if (timeoutMs <= 0) {
      throw new IllegalArgumentException("JDBC connect timeout must be positive");
    }
    this.delegate = delegate;
    this.timeoutMs = timeoutMs;
    this.workers = workers;
    this.cancellers = cancellers;
  }

  /**
   * {@inheritDoc}
   *
   * @throws SQLTimeoutException if the connection deadline expires or the caller is interrupted
   * @throws SQLTransientConnectionException if all workers are busy or a previous timed-out attempt
   *     through this driver is still running
   */
  @Override
  public Connection connect(String url, @Nullable Properties info) throws SQLException {
    if (abandonedAttempts.get() != 0) {
      throw new SQLTransientConnectionException(
          "A previous timed-out JDBC connection attempt is still running. Try again after it finishes.",
          "08001");
    }
    Attempt attempt = new Attempt();
    ClassLoader context = Thread.currentThread().getContextClassLoader();
    AccessControlContext callerContext = AccessController.getContext();
    Properties properties = info == null ? new Properties() : (Properties) info.clone();
    try {
      workers.execute(
          () ->
              AccessController.doPrivileged(
                  (PrivilegedAction<Void>)
                      () -> {
                        attempt.run(url, properties, context);
                        return null;
                      },
                  callerContext));
    } catch (RejectedExecutionException e) {
      logRejection();
      throw new SQLTransientConnectionException(
          "The JDBC connection attempt limit has been reached. Try again after an attempt finishes.",
          "08001",
          e);
    }
    try {
      Connection connection = attempt.result.get(timeoutMs, TimeUnit.MILLISECONDS);
      attempt.accept();
      return connection;
    } catch (TimeoutException e) {
      attempt.abandon();
      throw new SQLTimeoutException(
          "JDBC connection attempt exceeded "
              + timeoutMs
              + " ms. Check backend and proxy connectivity.",
          "08001",
          e);
    } catch (InterruptedException e) {
      attempt.abandon();
      Thread.currentThread().interrupt();
      throw new SQLTimeoutException("Interrupted while opening a JDBC connection", "08001", e);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof SQLException) {
        throw (SQLException) cause;
      }
      if (cause instanceof RuntimeException) {
        throw (RuntimeException) cause;
      }
      if (cause instanceof Error) {
        throw (Error) cause;
      }
      throw new SQLException("Failed to open JDBC connection", "08001", cause);
    } finally {
      attempt.decision.countDown();
    }
  }

  /** {@inheritDoc} */
  @Override
  public boolean acceptsURL(String url) throws SQLException {
    return delegate.acceptsURL(url);
  }

  /** {@inheritDoc} */
  @Override
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
    return delegate.getPropertyInfo(url, info);
  }

  /** {@inheritDoc} */
  @Override
  public int getMajorVersion() {
    return delegate.getMajorVersion();
  }

  /** {@inheritDoc} */
  @Override
  public int getMinorVersion() {
    return delegate.getMinorVersion();
  }

  /** {@inheritDoc} */
  @Override
  public boolean jdbcCompliant() {
    return delegate.jdbcCompliant();
  }

  /** {@inheritDoc} */
  @Override
  public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
    return delegate.getParentLogger();
  }

  /** Names the delegate driver in logs; the URL is omitted because it may carry credentials. */
  private String driverName() {
    return delegate.getClass().getName();
  }

  /** Logs rejected attempts at most once a minute, with the count since the last log. */
  private void logRejection() {
    REJECTIONS.incrementAndGet();
    long now = System.nanoTime();
    long last = LAST_REJECTION_LOG.get();
    if (now - last >= REJECTION_LOG_INTERVAL_NANOS && LAST_REJECTION_LOG.compareAndSet(last, now)) {
      LOG.warn(
          "Rejected {} JDBC connection attempt(s) since the last report, including one for {}: all "
              + "connection workers are busy, likely with abandoned attempts against an "
              + "unreachable backend",
          REJECTIONS.getAndSet(0),
          driverName());
    }
  }

  private static ExecutorService daemonPool(int maxThreads, String name) {
    return new ThreadPoolExecutor(
        0,
        maxThreads,
        60,
        TimeUnit.SECONDS,
        new SynchronousQueue<>(),
        task ->
            AccessController.doPrivileged(
                (PrivilegedAction<Thread>)
                    () -> {
                      Thread thread = new Thread(task, name);
                      thread.setDaemon(true);
                      thread.setContextClassLoader(BoundedConnectionDriver.class.getClassLoader());
                      return thread;
                    },
                WORKER_CONTEXT));
  }

  private final class Attempt {
    private final CompletableFuture<Connection> result = new CompletableFuture<>();
    private final CountDownLatch decision = new CountDownLatch(1);
    private final long startNanos = System.nanoTime();
    private boolean accepted;
    private boolean abandoned;
    private boolean finished;
    @Nullable private Thread worker;
    private boolean connecting;
    private boolean cancelling;

    synchronized void accept() {
      accepted = true;
    }

    void abandon() {
      synchronized (this) {
        if (finished) {
          return;
        }
        abandoned = true;
        abandonedAttempts.incrementAndGet();
      }
      LOG.warn(
          "Abandoned a {} connection attempt (deadline {} ms); interrupting its worker if it is "
              + "still connecting",
          driverName(),
          timeoutMs);
      try {
        // Thread.interrupt may synchronously run a driver's blocking channel-close callback, so
        // it must not run on this request thread, which would defeat the connection deadline.
        cancellers.execute(this::cancel);
      } catch (RejectedExecutionException e) {
        LOG.warn("Could not schedule interruption of an abandoned JDBC connection attempt", e);
      }
    }

    /** Interrupts the worker only while it is still inside {@link Driver#connect}. */
    private void cancel() {
      Thread target;
      synchronized (this) {
        if (!connecting) {
          return;
        }
        target = worker;
        cancelling = true;
      }
      try {
        target.interrupt();
      } finally {
        synchronized (this) {
          cancelling = false;
          notifyAll();
        }
      }
    }

    /** Ends the interruptible phase; waits for an in-flight interrupt to be delivered. */
    private synchronized void endConnect() {
      connecting = false;
      while (cancelling) {
        try {
          wait();
        } catch (InterruptedException e) {
          // The wait consumes the in-flight cancellation; the worker must not carry it into
          // cleanup or the next attempt.
        }
      }
    }

    void run(String url, Properties properties, @Nullable ClassLoader context) {
      Thread current = Thread.currentThread();
      ClassLoader previous = current.getContextClassLoader();
      current.setContextClassLoader(context);
      Connection connection = null;
      boolean interrupted = false;
      try {
        synchronized (this) {
          if (abandoned) {
            return;
          }
          worker = current;
          connecting = true;
        }
        try {
          connection = delegate.connect(url, properties);
        } finally {
          endConnect();
        }
        result.complete(connection);
        while (true) {
          try {
            decision.await();
            break;
          } catch (InterruptedException e) {
            // Cancellation must not bypass the handoff decision or discard a late connection.
            interrupted = true;
          }
        }
      } catch (Throwable e) {
        result.completeExceptionally(e);
      } finally {
        // Cancellation must not make an interrupt-sensitive close fail before releasing the
        // physical connection. Restore the signal only after cleanup, including delegate signals.
        interrupted |= Thread.interrupted();
        boolean delivered;
        synchronized (this) {
          delivered = accepted;
        }
        try {
          if (!delivered && connection != null) {
            try {
              connection.close();
            } catch (SQLException e) {
              LOG.warn("Failed to close an abandoned JDBC connection", e);
            }
          }
        } finally {
          synchronized (this) {
            finished = true;
            worker = null;
            if (abandoned) {
              abandonedAttempts.decrementAndGet();
              LOG.info(
                  "An abandoned {} connection attempt finished after {} ms",
                  driverName(),
                  TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos));
            }
          }
          current.setContextClassLoader(previous);
          if (interrupted) {
            current.interrupt();
          }
        }
      }
    }
  }
}
