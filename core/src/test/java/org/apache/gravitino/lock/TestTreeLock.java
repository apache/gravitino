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

import static org.apache.gravitino.lock.TestLockManager.getConfig;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;

import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class TestTreeLock {
  private LockManager lockManager;
  private TreeLock lock;

  @BeforeEach
  void setUp() {
    lockManager = new LockManager(getConfig());
    lock = lockManager.createTreeLock(TestLockManager.randomNameIdentifier());
  }

  @Test
  void testLockAndUnlockWithReadLock() {
    assertDoesNotThrow(
        () -> {
          lock.lock(LockType.READ);
          lock.unlock();
        });
  }

  @Test
  void testLockAndUnlockWithWriteLock() {
    assertDoesNotThrow(
        () -> {
          lock.lock(LockType.WRITE);
          lock.unlock();
        });
  }

  @Test
  void testUnlockWithoutLock() {
    assertThrows(
        IllegalStateException.class,
        () -> {
          lock.unlock();
        });
  }

  @Test
  void testMultipleLockAndUnlock() {
    assertDoesNotThrow(
        () -> {
          for (int i = 0; i < 1000; i++) {
            lock.lock(i % 2 == 0 ? LockType.READ : LockType.WRITE);
            lock.unlock();
          }
        });
  }

  @Test
  void testLockFailureAndUnlock() {
    TreeLockNode mockNode1 = Mockito.mock(TreeLockNode.class);
    TreeLockNode mockNode2 = Mockito.mock(TreeLockNode.class);
    TreeLockNode mockNode3 = Mockito.mock(TreeLockNode.class);

    // Mock the lock method of the second node to throw an exception
    doThrow(new RuntimeException("Mock exception")).when(mockNode2).lock(Mockito.any());

    List<TreeLockNode> lockNodes = Arrays.asList(mockNode1, mockNode2, mockNode3);
    TreeLock treeLock = new TreeLock(lockNodes, TestLockManager.randomNameIdentifier());

    assertThrows(
        RuntimeException.class,
        () -> treeLock.lock(LockType.WRITE),
        "Expected lock to throw, but it didn't");

    // Verify that the first node was unlocked
    Mockito.verify(mockNode1, Mockito.times(1)).unlock(Mockito.any());

    // Verify that the second and third nodes were not unlocked
    Mockito.verify(mockNode2, Mockito.never()).unlock(Mockito.any());
    Mockito.verify(mockNode3, Mockito.never()).unlock(Mockito.any());
  }

  @Test
  void testDoubleUnlockThrows() {
    lock.lock(LockType.READ);
    lock.unlock();
    assertThrows(IllegalStateException.class, () -> lock.unlock());
  }
}
