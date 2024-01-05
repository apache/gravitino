/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.lock;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Lock node is a node in the lock tree. It contains the lock and the read count. The read count is
 * used to track the number of read locks that have been acquired.
 */
public class LockNode implements AutoCloseable {
  private final ReentrantReadWriteLock lock;
  private final AtomicLong readCount = new AtomicLong();

  public LockNode() {
    // TODO(yuqi) Can be optimized here: reuse the lock instance to avoid creating the new lock
    // instance
    lock = new ReentrantReadWriteLock();
  }

  @Override
  public void close() throws Exception {
    // Release lock
  }

  /**
   * Lock the node with the given lock type.
   *
   * @param lockType The lock type to lock the node.
   */
  public void lock(LockType lockType) {
    if (lockType == LockType.READ) {
      lock.readLock().lock();
      readCount.incrementAndGet();
    } else {
      lock.writeLock().lock();
    }
  }

  /**
   * Release the lock with the given lock type.
   *
   * @param lockType The lock type to release the lock.
   */
  public void release(LockType lockType) {
    if (lockType == LockType.READ) {
      lock.readLock().unlock();
      readCount.decrementAndGet();
    } else {
      lock.writeLock().unlock();
    }
  }
}
