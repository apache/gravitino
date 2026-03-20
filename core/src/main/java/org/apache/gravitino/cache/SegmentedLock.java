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

package org.apache.gravitino.cache;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Striped;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Segmented lock for improved concurrency. Divides locks into segments to reduce contention.
 * Supports global clearing operations that require exclusive access to all segments.
 */
public class SegmentedLock {
  private static final Object NULL_KEY = new Object();

  private final Striped<Lock> stripedLocks;

  /**
   * Stable index for each stripe lock, built once at construction. Used to sort locks in {@link
   * #getDistinctSortedLocks} with a guaranteed total order (no hash-collision ties).
   */
  private final Map<Lock, Integer> lockIndices;

  /** CountDownLatch for global operations - null when no operation is in progress */
  private final AtomicReference<CountDownLatch> globalOperationLatch = new AtomicReference<>();

  /**
   * Creates a SegmentedLock with the specified number of segments. Guava's Striped automatically
   * rounds up to the nearest power of 2 for optimal performance.
   *
   * @param numSegments Number of segments (must be positive)
   * @throws IllegalArgumentException if numSegments is not positive
   */
  public SegmentedLock(int numSegments) {
    if (numSegments <= 0) {
      throw new IllegalArgumentException(
          "Number of segments must be positive, got: " + numSegments);
    }

    this.stripedLocks = Striped.lock(numSegments);
    this.lockIndices =
        IntStream.range(0, this.stripedLocks.size())
            .boxed()
            .collect(Collectors.toMap(this.stripedLocks::getAt, i -> i));
  }

  /**
   * Gets the segment lock for the given key.
   *
   * @param key Object to determine the segment
   * @return Segment lock for the key
   */
  public Lock getSegmentLock(Object key) {
    return stripedLocks.get(normalizeKey(key));
  }

  /**
   * Normalizes the key to handle null values consistently.
   *
   * @param key The input key
   * @return Normalized key (never null)
   */
  private Object normalizeKey(Object key) {
    return key != null ? key : NULL_KEY;
  }

  /**
   * Runs action with segment lock for the given key. Will wait if a global clearing operation is in
   * progress.
   *
   * @param key Key to determine segment
   * @param action Action to run
   * @throws RuntimeException if interrupted
   */
  public void withLock(Object key, Runnable action) {
    waitForGlobalComplete();
    Lock lock = getSegmentLock(key);
    try {
      lock.lockInterruptibly();
      try {
        action.run();
      } finally {
        lock.unlock();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Thread was interrupted while waiting for lock", e);
    }
  }

  /**
   * Runs action with segment lock and returns result. Will wait if a global clearing operation is
   * in progress.
   *
   * @param key Key to determine segment
   * @param action Action to run
   * @param <T> Result type
   * @return Action result
   * @throws RuntimeException if interrupted
   */
  public <T> T withLock(Object key, java.util.function.Supplier<T> action) {
    waitForGlobalComplete();
    Lock lock = getSegmentLock(key);
    try {
      lock.lockInterruptibly();
      try {
        return action.get();
      } finally {
        lock.unlock();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Thread was interrupted while waiting for lock", e);
    }
  }

  /**
   * Runs action with segment lock for the given key. Will wait if a global clearing operation is in
   * progress.
   *
   * @param key Key to determine segment
   * @param action Action to run
   * @param <T> Result type
   * @param <E> Exception type
   * @return Action result
   * @throws E Exception
   */
  public <T, E extends Exception> T withLockAndThrow(
      Object key, EntityCache.ThrowingSupplier<T, E> action) throws E {
    waitForGlobalComplete();
    Lock lock = getSegmentLock(key);
    try {
      lock.lockInterruptibly();
      try {
        return action.get();
      } finally {
        lock.unlock();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Thread was interrupted while waiting for lock", e);
    }
  }

  /**
   * Runs action with segment lock for the given key. Will wait if a global clearing operation is in
   * progress.
   *
   * @param key Key to determine segment
   * @param action Action to run
   * @param <E> Exception type
   * @throws E Exception
   */
  public <E extends Exception> void withLockAndThrow(
      Object key, EntityCache.ThrowingRunnable<E> action) throws E {
    waitForGlobalComplete();
    Lock lock = getSegmentLock(key);
    try {
      lock.lockInterruptibly();
      try {
        action.run();
      } finally {
        lock.unlock();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Thread was interrupted while waiting for lock", e);
    }
  }

  /**
   * Acquires locks for multiple keys in a consistent order (sorted by stripe index) to avoid
   * deadlocks, then executes the action.
   *
   * @param keys The keys to lock
   * @param action Action to run
   * @param <E> Exception type
   * @throws E Exception from the action
   */
  public <E extends Exception> void withMultipleKeyLockAndThrow(
      List<? extends Object> keys, EntityCache.ThrowingRunnable<E> action) throws E {
    waitForGlobalComplete();
    List<Lock> sortedLocks = getDistinctSortedLocks(keys);
    acquireAllLocks(sortedLocks);
    try {
      action.run();
    } finally {
      releaseAllLocks(sortedLocks);
    }
  }

  /**
   * Acquires locks for multiple keys in a consistent order (sorted by stripe index) to avoid
   * deadlocks, then executes the action and returns the result.
   *
   * @param keys The keys to lock
   * @param action Action to run
   * @param <T> Result type
   * @param <E> Exception type
   * @return Action result
   * @throws E Exception from the action
   */
  public <T, E extends Exception> T withMultipleKeyLockAndThrow(
      List<? extends Object> keys, EntityCache.ThrowingSupplier<T, E> action) throws E {
    waitForGlobalComplete();
    List<Lock> sortedLocks = getDistinctSortedLocks(keys);
    acquireAllLocks(sortedLocks);
    try {
      return action.get();
    } finally {
      releaseAllLocks(sortedLocks);
    }
  }

  /** Checks if a global operation is currently in progress. */
  @VisibleForTesting
  public boolean isClearing() {
    return globalOperationLatch.get() != null;
  }

  /**
   * Returns number of lock segments.
   *
   * @return Number of segments
   */
  public int getNumSegments() {
    return stripedLocks.size();
  }

  /**
   * Executes a global clearing operation with exclusive access to all segments. This method sets
   * the clearing flag and ensures no other operations can proceed until the clearing is complete.
   *
   * @param action The clearing action to execute
   */
  public void withGlobalLock(Runnable action) {
    // Create a new CountDownLatch for this operation
    CountDownLatch latch = new CountDownLatch(1);

    // Atomically set the latch, fail if another operation is already in progress
    if (!globalOperationLatch.compareAndSet(null, latch)) {
      throw new IllegalStateException("Global operation already in progress");
    }

    try {
      synchronized (this) {
        action.run();
      }
    } finally {
      // Clear state first, then signal completion
      globalOperationLatch.set(null);
      latch.countDown();
    }
  }

  private List<Lock> getDistinctSortedLocks(List<? extends Object> keys) {
    return keys.stream()
        .map(this::getSegmentLock)
        .distinct()
        .sorted(Comparator.comparingInt(lockIndices::get))
        .collect(Collectors.toList());
  }

  private void acquireAllLocks(List<Lock> locks) {
    int lockedCount = 0;
    try {
      for (Lock lock : locks) {
        lock.lockInterruptibly();
        lockedCount++;
      }
    } catch (InterruptedException e) {
      releaseLocks(locks, lockedCount);
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Thread was interrupted while acquiring batch lock", e);
    }
  }

  private static void releaseLocks(List<Lock> locks, int lockedCount) {
    for (int i = lockedCount - 1; i >= 0; i--) {
      locks.get(i).unlock();
    }
  }

  private void releaseAllLocks(List<Lock> locks) {
    releaseLocks(locks, locks.size());
  }

  /**
   * Waits for any ongoing global operation to complete. This method is called by regular operations
   * to ensure they don't interfere with global operations.
   */
  private void waitForGlobalComplete() {
    CountDownLatch latch = globalOperationLatch.get();
    if (latch != null) {
      try {
        latch.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(
            "Thread was interrupted while waiting for global operation to complete", e);
      }
    }
  }
}
