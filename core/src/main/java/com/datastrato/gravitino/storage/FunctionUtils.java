/*
 * Copyright 2023 Datastrato.
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
  public interface IOExecutable<R, E extends Exception> {

    R execute() throws IOException, E;
  }

  public static <R, E extends Exception> R executeWithReadLock(
      IOExecutable<R, E> executable, ReentrantReadWriteLock lock) throws E, IOException {
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

  public static <R, E extends Exception> R executeWithWriteLock(
      IOExecutable<R, E> executable, ReentrantReadWriteLock lock) throws E, IOException {
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
