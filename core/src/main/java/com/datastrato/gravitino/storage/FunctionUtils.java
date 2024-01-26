/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage;

import com.datastrato.gravitino.storage.kv.TransactionalKvBackend;
import com.datastrato.gravitino.utils.Executable;
import java.io.IOException;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/** Tools for executing functions with locks and transaction. */
public class FunctionUtils {

  private FunctionUtils() {}

  @FunctionalInterface
  public interface IOExecutable<R> {

    R execute() throws IOException;
  }

  public static <R> R executeWithReadLock(IOExecutable<R> executable, ReentrantReadWriteLock lock)
      throws IOException {
    // It already held the write lock
    if (lock.isWriteLockedByCurrentThread()) {
      return executable.execute();
    }

    lock.readLock().lock();
    try {
      return executable.execute();
    } finally {
      lock.readLock().unlock();
    }
  }

  public static <R> R executeWithWriteLock(IOExecutable<R> executable, ReentrantReadWriteLock lock)
      throws IOException {
    lock.writeLock().lock();
    try {
      return executable.execute();
    } finally {
      lock.writeLock().unlock();
    }
  }

  public static <R, E extends Exception> R executeInTransaction(
      Executable<R, E> executable, TransactionalKvBackend transactionalKvBackend)
      throws E, IOException {
    if (transactionalKvBackend.inTransaction()) {
      return executable.execute();
    }

    transactionalKvBackend.begin();
    try {
      R r = executable.execute();
      transactionalKvBackend.commit();
      return r;
    } finally {
      // Let GC do the roll-back work
      transactionalKvBackend.closeTransaction();
    }
  }
}
