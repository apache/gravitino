/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.lock;

import com.datastrato.gravitino.GravitinoEnv;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.utils.Executable;

/** Utility class for tree locks. */
public class TreeLockUtils {

  private TreeLockUtils() {
    // Prevent instantiation.
  }

  /**
   * Execute the given executable with the given tree lock.
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
    TreeLock lock = GravitinoEnv.getInstance().getLockManager().createTreeLock(identifier);
    try {
      lock.lock(lockType);
      return executable.execute();
    } finally {
      lock.unlock();
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
