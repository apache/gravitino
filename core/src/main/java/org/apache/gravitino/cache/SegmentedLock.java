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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Segmented lock for improved concurrency. Divides locks into segments to reduce contention.
 * Supports global clearing operations that require exclusive access to all segments.
 */
public class SegmentedLock {
  private static final Object NULL_KEY = new Object();

  private final Striped<Lock> stripedLocks;

  /**
   * Gates segment operations against global operations: segment operations hold the read lock
   * across their whole critical section, global operations hold the write lock, so a global
   * operation excludes every segment operation, including ones already in flight when it starts.
   * The lock is fair so that a steady stream of segment operations cannot indefinitely starve a
   * waiting global operation; reentrant read reacquisition is still permitted while a writer is
   * queued, so the nested cache paths that reacquire the read lock do not deadlock.
   */
  private final ReentrantReadWriteLock globalGate = new ReentrantReadWriteLock(true);

  /** True while a global operation is in progress, used to reject concurrent global operations. */
  private final AtomicBoolean clearing = new AtomicBoolean(false);

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
    Lock readLock = globalGate.readLock();
    try {
      readLock.lockInterruptibly();
      try {
        Lock lock = getSegmentLock(key);
        lock.lockInterruptibly();
        try {
          action.run();
        } finally {
          lock.unlock();
        }
      } finally {
        readLock.unlock();
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
    Lock readLock = globalGate.readLock();
    try {
      readLock.lockInterruptibly();
      try {
        Lock lock = getSegmentLock(key);
        lock.lockInterruptibly();
        try {
          return action.get();
        } finally {
          lock.unlock();
        }
      } finally {
        readLock.unlock();
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
    Lock readLock = globalGate.readLock();
    try {
      readLock.lockInterruptibly();
      try {
        Lock lock = getSegmentLock(key);
        lock.lockInterruptibly();
        try {
          return action.get();
        } finally {
          lock.unlock();
        }
      } finally {
        readLock.unlock();
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
    Lock readLock = globalGate.readLock();
    try {
      readLock.lockInterruptibly();
      try {
        Lock lock = getSegmentLock(key);
        lock.lockInterruptibly();
        try {
          action.run();
        } finally {
          lock.unlock();
        }
      } finally {
        readLock.unlock();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Thread was interrupted while waiting for lock", e);
    }
  }

  /** Checks if a global operation is currently in progress. */
  @VisibleForTesting
  public boolean isClearing() {
    return clearing.get();
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
   * <p>Exclusivity holds against every segment operation, including ones that were already in
   * flight when this method is called: segment operations hold the {@code globalGate} read lock
   * across their whole critical section, and the write lock acquired here waits for all of them.
   *
   * <p>Must not be called from inside a {@code withLock} action on the same instance: the read lock
   * cannot upgrade to the write lock, so such a call would deadlock. That case is rejected up front
   * rather than left to park forever.
   *
   * @param action The clearing action to execute
   * @throws IllegalStateException if the calling thread is already inside a {@code withLock} action
   *     on this instance, or if another global operation is in progress
   */
  public void withGlobalLock(Runnable action) {
    // A thread already inside a segment operation holds the gate read lock, which cannot upgrade
    // to the write lock below. Waiting for it would park this thread forever, so refuse instead:
    // a deadlocked test run reports nothing at all, while a failure names the offending call.
    if (globalGate.getReadHoldCount() > 0) {
      throw new IllegalStateException(
          "A global operation cannot run inside a withLock action on the same instance: the "
              + "segment operation's read lock cannot upgrade to the global write lock");
    }

    // Mark the global operation in progress, fail if another one is already running
    if (!clearing.compareAndSet(false, true)) {
      throw new IllegalStateException("Global operation already in progress");
    }

    try {
      globalGate.writeLock().lock();
      try {
        action.run();
      } finally {
        globalGate.writeLock().unlock();
      }
    } finally {
      clearing.set(false);
    }
  }
}
