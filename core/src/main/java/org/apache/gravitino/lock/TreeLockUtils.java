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

package org.apache.gravitino.lock;

import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.OptimisticLockException;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.utils.Executable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility class for tree locks. */
public class TreeLockUtils {

  private static final Logger LOG = LoggerFactory.getLogger(TreeLockUtils.class);

  /**
   * Maximum number of retry attempts when an {@link OptimisticLockException} is detected on a WRITE
   * operation. Each retry re-reads the entity and reapplies the change, then issues a fresh {@code
   * UPDATE ... WHERE current_version = N} to the entity store.
   */
  static final int OCC_MAX_RETRIES = 3;

  private TreeLockUtils() {
    // Prevent instantiation.
  }

  /**
   * Execute the given executable with the given tree lock.
   *
   * <p>For WRITE operations, if the entity store detects a concurrent modification via a {@code
   * current_version} mismatch and throws {@link OptimisticLockException}, the executable is retried
   * up to {@value #OCC_MAX_RETRIES} times with exponential backoff before the exception is
   * re-thrown. READ operations are not retried.
   *
   * @param identifier The identifier of resource path that the lock attempts to lock.
   * @param lockType The type of lock to use.
   * @param executable The executable to execute.
   * @return The result of the executable.
   * @param <R> The type of the result.
   * @param <E> The type of the exception.
   * @throws E If the executable throws an exception.
   */
  public static <R, E extends Exception> R doWithTreeLock(
      NameIdentifier identifier, LockType lockType, Executable<R, E> executable) throws E {
    TreeLock lock = GravitinoEnv.getInstance().lockManager().createTreeLock(identifier);
    try {
      lock.lock(lockType);
      if (lockType == LockType.READ) {
        return executable.execute();
      }
      for (int attempt = 0; ; attempt++) {
        try {
          return executable.execute();
        } catch (OptimisticLockException e) {
          if (attempt >= OCC_MAX_RETRIES) {
            throw e;
          }
          LOG.warn(
              "Optimistic lock conflict on {}, attempt {}/{}, retrying",
              identifier,
              attempt + 1,
              OCC_MAX_RETRIES);
          try {
            Thread.sleep(10L << attempt); // 10 ms, 20 ms, 40 ms
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new GravitinoRuntimeException(
                ie, "Interrupted during OCC retry backoff on %s", identifier);
          }
        }
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Execute the given executable with the root tree lock.
   *
   * @param lockType The type of lock to use.
   * @param executable The executable to execute.
   * @return The result of the executable.
   * @param <R> The type of the result.
   * @param <E> The type of the exception.
   * @throws E If the executable throws an exception.
   */
  public static <R, E extends Exception> R doWithRootTreeLock(
      LockType lockType, Executable<R, E> executable) throws E {
    return doWithTreeLock(LockManager.ROOT, lockType, executable);
  }
}
