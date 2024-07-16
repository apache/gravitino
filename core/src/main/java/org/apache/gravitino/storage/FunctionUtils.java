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

package org.apache.gravitino.storage;

import java.io.IOException;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.gravitino.storage.kv.TransactionalKvBackend;
import org.apache.gravitino.utils.Executable;

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
