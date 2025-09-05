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

import com.google.common.util.concurrent.Striped;
import java.util.concurrent.locks.Lock;

/** Segmented lock for improved concurrency. Divides locks into segments to reduce contention. */
public class SegmentedLock {
  private final Striped<Lock> stripedLocks;
  private static final Object NULL_KEY = new Object();

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
   * Runs action with segment lock for the given key.
   *
   * @param key Key to determine segment
   * @param action Action to run
   * @throws RuntimeException if interrupted
   */
  public void withLock(Object key, Runnable action) {
    Lock lock = getSegmentLock(key);
    try {
      lock.lockInterruptibly();
      action.run();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Thread was interrupted while waiting for lock", e);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Runs action with segment lock and returns result.
   *
   * @param key Key to determine segment
   * @param action Action to run
   * @param <T> Result type
   * @return Action result
   * @throws RuntimeException if interrupted
   */
  public <T> T withLock(Object key, java.util.function.Supplier<T> action) {
    Lock lock = getSegmentLock(key);
    try {
      lock.lockInterruptibly();
      return action.get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Thread was interrupted while waiting for lock", e);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Runs action with all segment locks.
   *
   * @param action Action to run
   */
  public void withAllLocks(Runnable action) {
    // Use a single global lock to ensure thread safety
    // This is safer than trying to lock all segments individually
    synchronized (this) {
      action.run();
    }
  }

  /**
   * Runs action with all segment locks and returns result.
   *
   * @param action Action to run
   * @param <T> Result type
   * @return Action result
   */
  public <T> T withAllLocks(java.util.function.Supplier<T> action) {
    // Use a single global lock to ensure thread safety
    // This is safer than trying to lock all segments individually
    synchronized (this) {
      return action.get();
    }
  }

  /**
   * Runs action with all segment locks and throws exception if it occurs.
   *
   * @param action Action to run
   * @param <T> Result type
   * @param <E> Exception type
   * @return Action result
   * @throws E If exception occurs
   */
  public <T, E extends Exception> T withAllLocksAndThrow(EntityCache.ThrowingSupplier<T, E> action)
      throws E {
    // Use a single global lock to ensure thread safety
    // This is safer than trying to lock all segments individually
    synchronized (this) {
      return action.get();
    }
  }

  /**
   * Runs action with all segment locks and throws exception if it occurs.
   *
   * @param action Action to run
   * @param <E> Exception type
   * @throws E If exception occurs
   */
  public <E extends Exception> void withAllLocksAndThrow(EntityCache.ThrowingRunnable<E> action)
      throws E {
    // Use a single global lock to ensure thread safety
    // This is safer than trying to lock all segments individually
    synchronized (this) {
      action.run();
    }
  }

  /**
   * Returns number of lock segments.
   *
   * @return Number of segments
   */
  public int getNumSegments() {
    return stripedLocks.size();
  }
}
