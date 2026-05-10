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
import org.apache.gravitino.utils.Executable;

/** Utility class for tree locks. */
public class TreeLockUtils {

  private TreeLockUtils() {
    // Prevent instantiation.
  }

  /**
   * Execute the given executable while holding a hierarchical lock on {@code identifier}.
   *
   * <p>The lock is acquired via the configured {@link LockBackend} (default: in-process). All
   * existing call sites are unchanged in semantics — see {@code design-docs/treelock-ha.md} for the
   * full backend story.
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
    LockBackend backend = GravitinoEnv.getInstance().lockManager().backend();
    try (LockHandle handle = backend.acquire(identifier, lockType)) {
      return executable.execute();
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
