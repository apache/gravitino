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
package org.apache.gravitino.catalog;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.lock.LockType;
import org.apache.gravitino.lock.TreeLock;
import org.apache.gravitino.lock.TreeLockNode;
import org.junit.jupiter.api.Assertions;

/**
 * Helpers for asserting how a dispatcher operation interacts with tree locks held by other threads.
 * A {@link HeldLock} pins a lock on a helper thread so a test can check whether an operation runs
 * concurrently with it or waits for it. Waiting is detected by observing the operation thread
 * parked inside {@link TreeLockNode}, not by sleeping, so the assertions do not depend on timing.
 */
final class TreeLockTestSupport {

  private static final Duration TIMEOUT = Duration.ofSeconds(30);

  private TreeLockTestSupport() {}

  /**
   * Asserts that {@code operation} completes while {@code held} is still held, i.e. the operation
   * does not contend with that lock.
   */
  static void assertRunsConcurrentlyWith(HeldLock held, Runnable operation) throws Exception {
    OperationRun run = OperationRun.start(operation);
    try {
      if (run.awaitDoneOrParked() == Outcome.PARKED) {
        held.release();
        Assertions.fail(
            "Operation parked on a tree lock while " + held + " was held; it contends with it");
      }
      run.join();
    } finally {
      run.shutdown();
    }
  }

  /**
   * Asserts that {@code operation} parks on a tree lock while {@code held} is held and completes
   * once the lock is released.
   */
  static void assertWaitsFor(HeldLock held, Runnable operation) throws Exception {
    OperationRun run = OperationRun.start(operation);
    try {
      Assertions.assertEquals(
          Outcome.PARKED,
          run.awaitDoneOrParked(),
          "Operation finished without waiting for " + held);
      held.release();
      run.join();
    } finally {
      run.shutdown();
    }
  }

  private enum Outcome {
    DONE,
    PARKED
  }

  /** An operation running on its own thread whose progress can be observed. */
  private static final class OperationRun {
    private final ExecutorService executor;
    private final Thread[] worker = new Thread[1];
    private Future<?> future;

    private OperationRun() {
      this.executor =
          Executors.newSingleThreadExecutor(
              r -> {
                worker[0] = new Thread(r, "tree-lock-test-operation");
                return worker[0];
              });
    }

    static OperationRun start(Runnable operation) {
      OperationRun run = new OperationRun();
      run.future = run.executor.submit(operation);
      return run;
    }

    /** Waits until the operation either completes or blocks inside {@link TreeLockNode#lock}. */
    Outcome awaitDoneOrParked() throws Exception {
      long deadline = System.nanoTime() + TIMEOUT.toNanos();
      while (System.nanoTime() < deadline) {
        if (future.isDone()) {
          return Outcome.DONE;
        }
        Thread thread = worker[0];
        if (thread != null && isParkedOnTreeLock(thread)) {
          return Outcome.PARKED;
        }
        Thread.sleep(5);
      }
      throw new TimeoutException("Operation neither finished nor parked on a tree lock");
    }

    void join() throws Exception {
      try {
        future.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
      } catch (ExecutionException e) {
        throw new AssertionError("Operation failed", e.getCause());
      }
    }

    void shutdown() {
      executor.shutdownNow();
    }

    private static boolean isParkedOnTreeLock(Thread thread) {
      Thread.State state = thread.getState();
      if (state != Thread.State.WAITING && state != Thread.State.TIMED_WAITING) {
        return false;
      }
      for (StackTraceElement frame : thread.getStackTrace()) {
        if (TreeLockNode.class.getName().equals(frame.getClassName())
            && "lock".equals(frame.getMethodName())) {
          return true;
        }
      }
      return false;
    }
  }

  /** A tree lock held by a dedicated thread until {@link #release()} or {@link #close()}. */
  static final class HeldLock implements AutoCloseable {
    private final NameIdentifier identifier;
    private final LockType lockType;
    private final CountDownLatch acquired = new CountDownLatch(1);
    private final CountDownLatch releaseSignal = new CountDownLatch(1);
    private final Thread holder;
    private volatile Throwable failure;

    private HeldLock(NameIdentifier identifier, LockType lockType) {
      this.identifier = identifier;
      this.lockType = lockType;
      this.holder = new Thread(this::hold, "tree-lock-test-holder");
    }

    /** Acquires the lock on a helper thread and returns once it is held. */
    static HeldLock acquire(NameIdentifier identifier, LockType lockType) throws Exception {
      HeldLock held = new HeldLock(identifier, lockType);
      held.holder.start();
      held.acquired.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
      if (held.failure != null) {
        throw new AssertionError("Failed to acquire " + held, held.failure);
      }
      return held;
    }

    private void hold() {
      TreeLock lock = GravitinoEnv.getInstance().lockManager().createTreeLock(identifier);
      try {
        lock.lock(lockType);
      } catch (Throwable t) {
        failure = t;
        acquired.countDown();
        return;
      }
      acquired.countDown();
      try {
        releaseSignal.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } finally {
        lock.unlock();
      }
    }

    /** Releases the lock and waits for the holder thread to exit. */
    void release() throws InterruptedException {
      releaseSignal.countDown();
      holder.join(TIMEOUT.toMillis());
    }

    @Override
    public void close() throws InterruptedException {
      release();
    }

    @Override
    public String toString() {
      return lockType + " lock on " + identifier;
    }
  }
}
