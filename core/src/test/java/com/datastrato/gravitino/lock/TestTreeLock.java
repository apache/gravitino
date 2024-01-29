/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.lock;

import org.junit.jupiter.api.Test;

class TestTreeLock {

  @Test
  void testLock() {
    LockManager lockManager = new LockManager();

    for (int i = 0; i < 1000; i++) {
      TreeLock lock = lockManager.createTreeLock(TestLockManager.randomNameIdentifier());
      lock.lock(i % 2 == 0 ? LockType.READ : LockType.WRITE);
      try {
        // Business logic here.
      } finally {
        lock.unlock();
      }
    }
  }
}
