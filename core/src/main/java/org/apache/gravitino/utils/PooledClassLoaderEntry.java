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
package org.apache.gravitino.utils;

import com.google.common.annotations.VisibleForTesting;

/**
 * An entry in the {@link ClassLoaderPool} that holds a shared {@link IsolatedClassLoader} and
 * tracks its reference count. When the reference count drops to zero, the ClassLoader can be safely
 * cleaned up and removed from the pool.
 *
 * <p><b>Threading contract:</b> {@link #incrementRefCount()} and {@link #decrementRefCount()} must
 * only be called inside {@link java.util.concurrent.ConcurrentHashMap#compute}, which serializes
 * access per key. A plain {@code int} is used instead of {@code AtomicInteger} to make this
 * contract explicit — misuse outside {@code compute()} will produce data races that are immediately
 * visible rather than silently incorrect.
 */
public class PooledClassLoaderEntry {

  private final ClassLoaderKey key;
  private final IsolatedClassLoader classLoader;
  private int refCount = 1;
  private boolean cleanedUp = false;

  /**
   * Constructs a PooledClassLoaderEntry.
   *
   * @param key The key identifying this ClassLoader in the pool.
   * @param classLoader The isolated ClassLoader instance.
   */
  public PooledClassLoaderEntry(ClassLoaderKey key, IsolatedClassLoader classLoader) {
    this.key = key;
    this.classLoader = classLoader;
  }

  /** Returns the key identifying this ClassLoader in the pool. */
  public ClassLoaderKey key() {
    return key;
  }

  /** Returns the isolated ClassLoader instance. */
  public IsolatedClassLoader classLoader() {
    return classLoader;
  }

  /**
   * Returns the current reference count. Note: outside of {@code ConcurrentHashMap.compute()}, the
   * returned value may be stale.
   */
  @VisibleForTesting
  int refCount() {
    return refCount;
  }

  /**
   * Increments the reference count and returns the new value. Must only be called inside {@link
   * java.util.concurrent.ConcurrentHashMap#compute}.
   */
  int incrementRefCount() {
    return ++refCount;
  }

  /**
   * Decrements the reference count and returns the new value. Must only be called inside {@link
   * java.util.concurrent.ConcurrentHashMap#compute}.
   */
  int decrementRefCount() {
    if (refCount <= 0) {
      throw new IllegalStateException("Cannot decrement refCount below zero for key: " + key);
    }
    return --refCount;
  }

  /**
   * Marks this entry as cleaned up. Returns {@code true} if this is the first call, {@code false}
   * if cleanup was already performed. Used as a defense-in-depth guard against double cleanup.
   */
  boolean markCleanedUp() {
    if (cleanedUp) {
      return false;
    }
    cleanedUp = true;
    return true;
  }
}
