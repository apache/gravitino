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

import org.apache.gravitino.NameIdentifier;

/**
 * The default {@link LockBackend}: a thin wrapper over the in-JVM {@link TreeLock} machinery owned
 * by {@link LockManager}. Behavior is unchanged from the pre-SPI implementation.
 *
 * <p>This backend provides no cross-process coordination and therefore is unsuitable for HA
 * deployments — see {@link JdbcLockBackend} for that.
 */
final class InProcessLockBackend implements LockBackend {

  static final String NAME = "inprocess";

  private final LockManager manager;

  InProcessLockBackend(LockManager manager) {
    this.manager = manager;
  }

  @Override
  public LockHandle acquire(NameIdentifier identifier, LockType lockType) {
    TreeLock lock = manager.createTreeLock(identifier);
    try {
      lock.lock(lockType);
    } catch (RuntimeException e) {
      // TreeLock.lock() rolls back partial acquisitions internally on failure; the references
      // taken by createTreeLock are released by that internal unlock(). Nothing to do here.
      throw e;
    }
    return new TreeLockHandle(lock);
  }

  @Override
  public String name() {
    return NAME;
  }

  /**
   * Idempotent handle wrapping a single {@link TreeLock}.
   *
   * <p>Visible-for-testing because some legacy lock tests exercise the underlying {@link TreeLock}
   * directly; nothing here is part of the public SPI.
   */
  private static final class TreeLockHandle implements LockHandle {
    private final TreeLock lock;
    private boolean closed = false;

    TreeLockHandle(TreeLock lock) {
      this.lock = lock;
    }

    @Override
    public synchronized void close() {
      if (closed) {
        return;
      }
      closed = true;
      lock.unlock();
    }
  }
}
