/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.lock;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.catalog.CatalogManager.CatalogWrapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.Stack;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * LockManager is a lock manager that manages the lock of the resource path. It will lock the whole
 * path from root to the resource path.
 *
 * <p>Assuming we need to alter the table `metalake.catalog.db1.table1`, the lock manager will lock
 * the following
 *
 * <pre>
 *   /                                    readLock
 *   /metalake                            readLock
 *   /metalake/catalog                    readLock
 *   /metalake/catalog/db1                readLock
 *   /metalake/catalog/db/table1          writeLock
 * </pre>
 *
 * If the lock manager fails to lock the resource path, it will release all the locks that have been
 * locked.
 */
public class LockManager {
  public static final Logger LOG = LoggerFactory.getLogger(LockManager.class);
  public static final long TIME_AFTER_LAST_ACCESS_TO_EVICT_IN_MS = 24 * 60 * 60 * 1000; // 1 day

  private static final NameIdentifier ROOT = NameIdentifier.ROOT;
  private final ThreadLocal<Stack<LockObject>> currentLocked = ThreadLocal.withInitial(Stack::new);
  private final Cache<NameIdentifier, LockNode> lockNodeMap;

  public LockManager() {
    this.lockNodeMap =
        Caffeine.newBuilder()
            .expireAfterAccess(TIME_AFTER_LAST_ACCESS_TO_EVICT_IN_MS, TimeUnit.MILLISECONDS)
            .maximumSize(100000)
            .evictionListener(
                (k, v, c) -> {
                  LOG.info("Evicting lockNode for {}.", k);
                  ((CatalogWrapper) v).close();
                })
            .removalListener(
                (k, v, c) -> {
                  LOG.info("Removing lockNode for {}.", k);
                  ((CatalogWrapper) v).close();
                })
            .scheduler(
                Scheduler.forScheduledExecutorService(
                    new ScheduledThreadPoolExecutor(
                        1,
                        new ThreadFactoryBuilder()
                            .setDaemon(true)
                            .setNameFormat("lock-cleaner-%d")
                            .build())))
            .build();

    lockNodeMap.put(ROOT, new LockNode());
  }

  static class LockObject {
    private final LockNode lockNode;
    private final LockType lockType;

    private LockObject(LockNode lockNode, LockType lockType) {
      this.lockNode = lockNode;
      this.lockType = lockType;
    }

    static LockObject of(LockNode lockNode, LockType lockType) {
      return new LockObject(lockNode, lockType);
    }
  }

  private Stack<NameIdentifier> getResourcePath(NameIdentifier identifier) {
    Stack<NameIdentifier> nameIdentifiers = new Stack<>();
    if (identifier == NameIdentifier.ROOT) {
      return nameIdentifiers;
    }

    NameIdentifier ref = identifier;
    do {
      ref = ref.parent();
      nameIdentifiers.push(ref);
    } while (ref.hasParent());

    return nameIdentifiers;
  }

  @VisibleForTesting
  LockNode getOrCreateLockNode(NameIdentifier identifier) {
    LockNode lockNode = lockNodeMap.asMap().get(identifier);
    if (lockNode == null) {
      synchronized (this) {
        lockNode = lockNodeMap.asMap().get(identifier);
        if (lockNode == null) {
          lockNode = new LockNode();
          lockNodeMap.put(identifier, lockNode);
        }
      }
    }
    return lockNode;
  }

  /**
   * Lock the resource path from root to the resource path.
   *
   * @param identifier The resource path to lock
   * @param lockType The lock type to lock the resource path.
   */
  public void lockResourcePath(NameIdentifier identifier, LockType lockType) {
    Stack<NameIdentifier> parents = getResourcePath(identifier);

    try {
      // Lock parent with read lock
      while (!parents.isEmpty()) {
        NameIdentifier parent = parents.pop();
        LockNode lockNode = getOrCreateLockNode(parent);
        lockNode.lock(LockType.READ);
        addLockToStack(lockNode, LockType.READ);
      }

      // Lock self with the value of `lockType`
      LockNode node = getOrCreateLockNode(identifier);
      node.lock(lockType);
      addLockToStack(node, lockType);
    } catch (Exception e) {
      // Unlock those that have been locked.
      unlockResourcePath();
      LOG.error("Failed to lock resource path {}", identifier, e);
      throw e;
    }
  }

  /**
   * Unlock the resource path from root to the resource path. We would get the resource path from
   * {@link ThreadLocal} instance.
   */
  public void unlockResourcePath() {
    Stack<LockObject> stack = currentLocked.get();
    while (!stack.isEmpty()) {
      LockObject lockObject = stack.pop();
      lockObject.lockNode.release(lockObject.lockType);
    }
  }

  private void addLockToStack(LockNode lockNode, LockType lockType) {
    currentLocked.get().push(LockObject.of(lockNode, lockType));
  }
}
