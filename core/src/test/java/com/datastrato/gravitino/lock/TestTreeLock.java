/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.lock;

import static com.datastrato.gravitino.lock.TestLockManager.getConfig;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
    assertDoesNotThrow(() -> {
      lock.lock(LockType.READ);
      lock.unlock();
    });
  }

  @Test
  void testLockAndUnlockWithWriteLock() {
    assertDoesNotThrow(() -> {
      lock.lock(LockType.WRITE);
      lock.unlock();
    });
  }

  @Test
  void testUnlockWithoutLock() {
    assertThrows(IllegalStateException.class, () -> {
      lock.unlock();
    });
  }

  @Test
  void testMultipleLockAndUnlock() {
    assertDoesNotThrow(() -> {
      for (int i = 0; i < 1000; i++) {
        lock.lock(i % 2 == 0 ? LockType.READ : LockType.WRITE);
        lock.unlock();
      }
    });
  }
}