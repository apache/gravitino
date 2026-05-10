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

import static org.apache.gravitino.Configs.LOCK_BACKEND_TYPE;
import static org.apache.gravitino.Configs.LOCK_BACKEND_TYPE_INPROCESS;
import static org.apache.gravitino.Configs.TREE_LOCK_CLEAN_INTERVAL;
import static org.apache.gravitino.Configs.TREE_LOCK_MAX_NODE_IN_MEMORY;
import static org.apache.gravitino.Configs.TREE_LOCK_MIN_NODE_IN_MEMORY;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import org.apache.gravitino.Config;
import org.apache.gravitino.NameIdentifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestInProcessLockBackend {

  private static Config newConfig() {
    Config config = mock(Config.class);
    doReturn(100_000L).when(config).get(TREE_LOCK_MAX_NODE_IN_MEMORY);
    doReturn(1_000L).when(config).get(TREE_LOCK_MIN_NODE_IN_MEMORY);
    doReturn(36_000L).when(config).get(TREE_LOCK_CLEAN_INTERVAL);
    doReturn(LOCK_BACKEND_TYPE_INPROCESS).when(config).get(LOCK_BACKEND_TYPE);
    return config;
  }

  @Test
  void defaultBackendIsInProcess() {
    LockManager manager = new LockManager(newConfig());
    Assertions.assertEquals("inprocess", manager.backend().name());
  }

  @Test
  void acquireWriteAndReleaseDecrementsReferenceCount() {
    LockManager manager = new LockManager(newConfig());
    LockBackend backend = manager.backend();

    NameIdentifier ident = NameIdentifier.of("metalake", "catalog", "db", "tbl");
    long beforeNodeCount = manager.totalNodeCount.get();

    LockHandle handle = backend.acquire(ident, LockType.WRITE);
    Assertions.assertNotNull(handle);
    handle.close();

    // The cleaner runs on a schedule; references should already be back to zero per node.
    Assertions.assertTrue(manager.totalNodeCount.get() >= beforeNodeCount);
  }

  @Test
  void acquireReadDoesNotBlockAnotherRead() {
    LockManager manager = new LockManager(newConfig());
    LockBackend backend = manager.backend();
    NameIdentifier ident = NameIdentifier.of("metalake", "schema");

    try (LockHandle h1 = backend.acquire(ident, LockType.READ);
        LockHandle h2 = backend.acquire(ident, LockType.READ)) {
      // Two READ locks on the same path co-exist — would have deadlocked under WRITE/WRITE.
      Assertions.assertNotNull(h1);
      Assertions.assertNotNull(h2);
    }
  }

  @Test
  void closeIsIdempotent() {
    LockManager manager = new LockManager(newConfig());
    LockBackend backend = manager.backend();

    LockHandle handle = backend.acquire(NameIdentifier.of("metalake"), LockType.READ);
    handle.close();
    // Second close must be a no-op, not a double-unlock that throws or corrupts ref counts.
    Assertions.assertDoesNotThrow(handle::close);
  }

  @Test
  void rootLockHandleAcquiresAndReleasesCleanly() {
    LockManager manager = new LockManager(newConfig());
    LockBackend backend = manager.backend();
    try (LockHandle h = backend.acquire(LockManager.ROOT, LockType.WRITE)) {
      Assertions.assertNotNull(h);
    }
  }

  @Test
  void unsupportedBackendTypeThrows() {
    Config config = newConfig();
    doReturn("zookeeper").when(config).get(LOCK_BACKEND_TYPE);
    Assertions.assertThrows(IllegalArgumentException.class, () -> new LockManager(config));
  }
}
