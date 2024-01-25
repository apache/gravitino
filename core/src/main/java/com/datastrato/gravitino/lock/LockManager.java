/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.lock;

import com.datastrato.gravitino.NameIdentifier;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.Stack;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.ArrayUtils;
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
 * locked in the inverse sequences it locks the resource path.
 */
public class LockManager {
  private static final Logger LOG = LoggerFactory.getLogger(LockManager.class);
  private static final long TIME_AFTER_LAST_ACCESS_TO_EVICT_IN_MS = 5 * 60 * 1000L; // 1 hour

  @VisibleForTesting final TreeLockNode treeLockRootNode = new TreeLockNode(NameIdentifier.ROOT);
  private final ThreadLocal<Stack<TreeLockPair>> holdingLocks = ThreadLocal.withInitial(Stack::new);
  private final ScheduledThreadPoolExecutor lockCleaner;

  static class TreeLockPair {
    private final TreeLockNode treeLockNode;
    private final LockType lockType;

    public TreeLockPair(TreeLockNode treeLockNode, LockType lockType) {
      this.treeLockNode = treeLockNode;
      this.lockType = lockType;
    }

    public static TreeLockPair of(TreeLockNode treeLockNode, LockType lockType) {
      return new TreeLockPair(treeLockNode, lockType);
    }
  }

  public LockManager() {
    this.lockCleaner =
        new ScheduledThreadPoolExecutor(
            1,
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("tree-lock-cleaner-%d")
                .build());

    lockCleaner.scheduleAtFixedRate(
        () ->
            treeLockRootNode
                .getAllChildren()
                .forEach(
                    child ->
                        evictStaleNodes(
                            TIME_AFTER_LAST_ACCESS_TO_EVICT_IN_MS, child, treeLockRootNode)),
        1,
        1,
        TimeUnit.MINUTES);
  }

  /**
   * Evict the stale nodes from the tree lock node if the last access time is earlier than the given
   * time.
   *
   * @param timeAfterLastAccessToEvictInMs The time after last access to evict the node.
   * @param treeNode The tree lock node to evict.
   * @param parent The parent of the tree lock node.
   */
  @VisibleForTesting
  void evictStaleNodes(
      long timeAfterLastAccessToEvictInMs, TreeLockNode treeNode, TreeLockNode parent) {
    if (treeNode.getLastAccessTime()
        < System.currentTimeMillis() - timeAfterLastAccessToEvictInMs) {
      // We need to assure that during the getLastAccessTime and removeChild, no other thread
      // can access and change the tree node.
      boolean success = treeNode.tryLock(LockType.WRITE);
      if (!success) {
        // This node is locked by another thread, we will skip it and try to evict its children.
        treeNode
            .getAllChildren()
            .forEach(child -> evictStaleNodes(timeAfterLastAccessToEvictInMs, child, treeNode));
        return;
      }

      try {
        // Double check.
        if (treeNode.getLastAccessTime()
            < System.currentTimeMillis() - timeAfterLastAccessToEvictInMs) {
          LOG.info(
              "Evicting stale node {} with last access time {}",
              treeNode.getIdent(),
              treeNode.getLastAccessTime());
          parent.removeChild(treeNode.getIdent());
        }
      } finally {
        treeNode.unlock(LockType.WRITE);
      }
    } else {
      treeNode
          .getAllChildren()
          .forEach(child -> evictStaleNodes(timeAfterLastAccessToEvictInMs, child, treeNode));
    }
  }

  /**
   * Lock the resource path from root to the resource path.
   *
   * @param identifier The resource path to lock
   * @param lockType The lock type to lock the resource path.
   */
  public void lockResourcePath(NameIdentifier identifier, LockType lockType) {
    TreeLockNode lockNode = treeLockRootNode;
    String[] levels = identifier.namespace().levels();

    try {
      for (int i = 0; i < levels.length; i++) {
        NameIdentifier ident = NameIdentifier.of(ArrayUtils.subarray(levels, 0, i + 1));
        lockNode = lockNode.getOrCreateChild(ident);
        lockNode.lock(LockType.READ);
        putLockToStack(lockNode, LockType.READ);
      }

      lockNode = lockNode.getOrCreateChild(identifier);
      lockNode.lock(lockType);
      putLockToStack(lockNode, lockType);
    } catch (Exception e) {
      unlockResourcePath();
      LOG.error("Failed to lock resource path {}", identifier, e);
      throw e;
    }
  }

  private void putLockToStack(TreeLockNode lockNode, LockType lockType) {
    Stack<TreeLockPair> stack = holdingLocks.get();
    stack.push(TreeLockPair.of(lockNode, lockType));
  }

  /**
   * Unlock the resource path from root to the resource path. We would get the locks holds by the
   * current thread.
   */
  public void unlockResourcePath() {
    try {
      Stack<TreeLockPair> stack = holdingLocks.get();
      while (!stack.isEmpty()) {
        TreeLockPair treeLockPair = stack.pop();
        treeLockPair.treeLockNode.unlock(treeLockPair.lockType);
      }
    } finally {
      holdingLocks.get().clear();
    }
  }
}
