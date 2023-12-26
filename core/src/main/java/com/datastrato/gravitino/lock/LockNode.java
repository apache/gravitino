/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.lock;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LockNode implements AutoCloseable {
  private final ReentrantReadWriteLock lock;
  private AtomicLong readCount = new AtomicLong();

  public LockNode() {
    // Can be optimized here: reuse the lock instance to avoid creating the new lock instance
    lock = new ReentrantReadWriteLock();
  }

  @Override
  public void close() throws Exception {
    // Release lock
  }

  public void lock(LockType lockType) {
    if (lockType == LockType.READ) {
      lock.readLock().lock();
      readCount.incrementAndGet();
    } else {
      lock.writeLock().lock();
    }
  }

  public void release(LockType lockType) {
    if (lockType == LockType.READ) {
      lock.readLock().unlock();
      readCount.decrementAndGet();
    } else {
      lock.writeLock().unlock();
    }
  }
}
