/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *  http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.cache;

import com.google.common.base.Preconditions;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.gravitino.NameIdentifier;

/**
 * A segmented lock manager that reduces lock contention by distributing locks across multiple
 * segments based on the hash code of a {@link NameIdentifier}.
 *
 * <p>Instead of using a single global lock, this class uses an array of {@link ReentrantLock}
 * instances. Each identifier is mapped to a specific lock using its hash code modulo the number of
 * segments. This allows concurrent operations on identifiers that map to different segments,
 * improving overall throughput in high-concurrency scenarios.
 *
 * <p>This implementation is thread-safe and immutable after construction. The number of segments
 * remains fixed for the lifetime of the instance.
 */
public class SegmentedLock {
  private final ReentrantLock[] locks;

  public SegmentedLock(int numSegments) {
    locks = new ReentrantLock[numSegments];
    for (int i = 0; i < numSegments; i++) {
      locks[i] = new ReentrantLock();
    }
  }

  /**
   * Returns the lock associated with the given identifier. The lock is determined by computing the
   * hash code of the identifier and mapping it to a segment using modulo arithmetic.
   *
   * <p>Note: {@link NameIdentifier#hashCode()} is assumed to provide a reasonably uniform
   * distribution to ensure balanced lock utilization.
   *
   * @param ident the identifier to map to a lock; must not be null
   * @return the {@link ReentrantLock} associated with the identifier
   */
  public ReentrantLock getLock(NameIdentifier ident) {
    Preconditions.checkNotNull(ident, "NameIdentifier can not be null.");
    int hash = ident.hashCode();
    int segmentIndex = Math.abs(hash % locks.length);
    return locks[segmentIndex];
  }

  /**
   * Executes the given action with all locks acquired. This method acquires all segment locks in
   * sequence before running the action, and ensures all locks are released afterward, even if the
   * action throws an exception.
   *
   * <p>This is useful for operations that need global consistency across all segments, such as
   * clearing all cached entries or performing a global snapshot. Use sparingly, as it blocks all
   * other operations.
   *
   * @param action the task to execute; must not be null
   * @throws NullPointerException if {@code action} is null
   */
  public void withAllLocks(Runnable action) {
    for (ReentrantLock lock : locks) {
      lock.lock();
    }
    try {
      action.run();
    } finally {
      for (ReentrantLock lock : locks) {
        lock.unlock();
      }
    }
  }
}
