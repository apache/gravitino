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

import java.io.Closeable;
import java.net.URLClassLoader;
import java.sql.Driver;
import java.sql.DriverManager;
import java.util.Enumeration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A pool that manages shared {@link IsolatedClassLoader} instances across catalogs of the same
 * type. Catalogs with identical provider, package properties, authorization plugin paths, and
 * Kerberos identity share the same ClassLoader, significantly reducing Metaspace memory usage.
 *
 * <p>Thread safety is guaranteed through {@link ConcurrentHashMap#compute} for all acquire/release
 * operations.
 */
public class ClassLoaderPool implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(ClassLoaderPool.class);

  private final ConcurrentHashMap<ClassLoaderKey, PooledClassLoaderEntry> pool =
      new ConcurrentHashMap<>();

  private final AtomicBoolean closed = new AtomicBoolean(false);

  /**
   * Acquires a ClassLoader entry for the given key. If an entry already exists, increments the
   * reference count. Otherwise, creates a new entry using the provided factory.
   *
   * @param key The key identifying the ClassLoader configuration.
   * @param factory A supplier that creates a new IsolatedClassLoader when needed.
   * @return The pooled ClassLoader entry.
   * @throws IllegalStateException if the pool has been closed.
   */
  public PooledClassLoaderEntry acquire(ClassLoaderKey key, Supplier<IsolatedClassLoader> factory) {
    return pool.compute(
        key,
        (k, existing) -> {
          if (closed.get()) {
            throw new IllegalStateException("ClassLoaderPool is already closed");
          }
          if (existing != null) {
            existing.incrementRefCount();
            LOG.debug("Reusing ClassLoader for key {}, refCount={}.", key, existing.refCount());
            return existing;
          }
          // If the factory throws (e.g., invalid classpath), the exception propagates to the
          // caller and ConcurrentHashMap leaves the key unmapped.
          IsolatedClassLoader classLoader = factory.get();
          PooledClassLoaderEntry newEntry = new PooledClassLoaderEntry(k, classLoader);
          LOG.info("Created new ClassLoader for key {}, refCount=1.", key);
          return newEntry;
        });
  }

  /**
   * Releases a ClassLoader entry. Decrements the reference count and, if it reaches zero, performs
   * final cleanup and removes the entry from the pool.
   *
   * @param entry The pooled ClassLoader entry to release.
   */
  public void release(PooledClassLoaderEntry entry) {
    pool.compute(
        entry.key(),
        (k, existing) -> {
          if (existing == null) {
            LOG.warn("Attempted to release a ClassLoader entry that is not in the pool: {}", k);
            return null;
          }
          int newCount = existing.decrementRefCount();
          LOG.debug("Released ClassLoader for key {}, refCount={}.", k, newCount);
          if (newCount <= 0) {
            doFinalCleanup(existing);
            return null; // Remove from map
          }
          return existing;
        });
  }

  /**
   * Returns the current number of distinct ClassLoader entries in the pool.
   *
   * @return The pool size.
   */
  public int size() {
    return pool.size();
  }

  /**
   * Closes the pool and cleans up all ClassLoader entries. After this method returns, {@link
   * #acquire} will throw {@link IllegalStateException}. Uses per-key {@code compute()} to ensure
   * atomic removal and cleanup, preventing races with concurrent release operations.
   */
  @Override
  public void close() {
    closed.set(true);
    // Drain with a loop to catch entries inserted by concurrent acquire() calls that were
    // already past the closed check when we set the flag. Since closed=true prevents any new
    // entries from being created, this loop is guaranteed to terminate.
    while (!pool.isEmpty()) {
      pool.keySet().forEach(this::removeAndCleanup);
    }
  }

  private void removeAndCleanup(ClassLoaderKey key) {
    pool.compute(
        key,
        (k, existing) -> {
          if (existing != null) {
            if (existing.refCount() > 0) {
              LOG.warn(
                  "Force-closing ClassLoader for key {} with {} active reference(s)"
                      + " (pool shutting down).",
                  k,
                  existing.refCount());
            } else {
              LOG.info("Closing pooled ClassLoader for key {}.", k);
            }
            doFinalCleanup(existing);
          }
          return null;
        });
  }

  /**
   * Performs final cleanup when a ClassLoader's reference count reaches zero. This includes:
   *
   * <ol>
   *   <li>Deregistering all JDBC drivers loaded by the ClassLoader
   *   <li>Cleaning up ClassLoader resources (ThreadLocals, Hadoop FileSystem, etc.)
   *   <li>Closing the ClassLoader itself
   * </ol>
   */
  private void doFinalCleanup(PooledClassLoaderEntry entry) {
    if (!entry.markCleanedUp()) {
      LOG.debug("ClassLoader for key {} already cleaned up, skipping.", entry.key());
      return;
    }

    IsolatedClassLoader isolatedClassLoader = entry.classLoader();
    try {
      URLClassLoader internalCl = isolatedClassLoader.getInternalClassLoader();
      if (internalCl != null) {
        deregisterAllDrivers(internalCl);
        ClassLoaderResourceCleanerUtils.closeClassLoaderResource(internalCl);
      }
    } catch (Exception e) {
      LOG.warn("Error during ClassLoader resource cleanup for key {}", entry.key(), e);
    }

    isolatedClassLoader.close();
    LOG.info("ClassLoader for key {} has been fully cleaned up.", entry.key());
  }

  /**
   * Deregisters all JDBC drivers that were loaded by the given ClassLoader.
   *
   * @param classLoader The ClassLoader whose drivers should be deregistered.
   */
  private void deregisterAllDrivers(ClassLoader classLoader) {
    // DriverManager.getDrivers() returns a snapshot in JDK 9+, so iterating while
    // calling deregisterDriver() is safe.
    Enumeration<Driver> drivers = DriverManager.getDrivers();
    while (drivers.hasMoreElements()) {
      Driver driver = drivers.nextElement();
      if (driver.getClass().getClassLoader() == classLoader) {
        try {
          DriverManager.deregisterDriver(driver);
          LOG.info("Deregistered JDBC driver {} for ClassLoader.", driver);
        } catch (Exception e) {
          LOG.warn("Failed to deregister JDBC driver {}", driver, e);
        }
      }
    }
  }
}
