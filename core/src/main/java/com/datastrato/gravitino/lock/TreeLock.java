/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.lock;

import com.datastrato.gravitino.NameIdentifier;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TreeLock is a lock that manages the lock process of the resource path. It will lock the whole
 * path from root to the resource path.
 *
 * <p>Assuming we need to load the table `metalake.catalog.db1.table1`, the lock manager will lock
 * the following
 *
 * <pre>
 *   /                                    readLock
 *   /metalake                            readLock
 *   /metalake/catalog                    readLock
 *   /metalake/catalog/db1                readLock
 *   /metalake/catalog/db/table1          readLock
 * </pre>
 *
 * If we need to alter a table `metalake.catalog.db1.table1` (without changing the name of it), the
 * lock manager will lock the following:
 *
 * <pre>
 *   /                                    readLock
 *   /metalake                            readLock
 *   /metalake/catalog                    readLock
 *   /metalake/catalog/db1                readLock
 *   /metalake/catalog/db/table1          writeLock
 * </pre>
 *
 * When we need to rename a table or drop a table `metalake.catalog.db1.table1`, the lock manager
 * will lock the following:
 *
 * <pre>
 *   /                                    readLock
 *   /metalake                            readLock
 *   /metalake/catalog                    readLock
 *   /metalake/catalog/db1                writeLock
 * </pre>
 *
 * If the lock manager fails to lock the resource path, it will release all the locks that have been
 * locked in the inverse sequences it locks the resource path.
 *
 * <p>The core of {@link TreeLock} is {@link TreeLockNode}. A TreeLock will hold several tree lock
 * nodes, all treeLock nodes shared by all tree lock instances will be stored in the {@link
 * LockManager} and can be reused later.
 */
public class TreeLock {
  public static final Logger LOG = LoggerFactory.getLogger(TreeLock.class);

  // The name identifier of the resource path.
  private final NameIdentifier identifier;
  // TreeLockNode to be locked
  private final List<TreeLockNode> lockNodes;

  // TreeLockNode that has been locked along with its lock type.
  private final Deque<Pair<TreeLockNode, LockType>> heldLocks = new ConcurrentLinkedDeque<>();
  private LockType lockType;

  TreeLock(List<TreeLockNode> lockNodes, NameIdentifier identifier) {
    this.lockNodes = lockNodes;
    this.identifier = identifier;
  }

  /**
   * Lock the tree lock with the given lock type. This method locks all nodes in the list, from the
   * root to the leaf, and pushes them onto the deque. If an exception occurs during the locking
   * process, it will unlock all nodes that have been locked so far.
   *
   * @param lockType The lock type to lock the tree lock.
   */
  public void lock(LockType lockType) {
    this.lockType = lockType;

    int length = lockNodes.size();
    for (int i = 0; i < length; i++) {
      TreeLockNode treeLockNode = lockNodes.get(i);
      LockType type = i == length - 1 ? lockType : LockType.READ;
      try {
        treeLockNode.lock(type);
        heldLocks.push(Pair.of(treeLockNode, type));
        if (LOG.isTraceEnabled()) {
          LOG.trace("Locked node: {}, lock type: {}", treeLockNode, type);
        }
      } catch (Exception e) {
        LOG.error(
            "Failed to lock the treeNode, identifier: {}, node {} of lockNodes: [{}]",
            identifier,
            treeLockNode,
            lockNodes,
            e);
        // unlock all nodes that have been locked when an exception occurs.
        unlock();
        throw e;
      }
    }

    if (LOG.isTraceEnabled()) {
      LOG.trace(
          "Locked the tree lock, ident: {}, lockNodes: [{}], lock type: {}",
          identifier,
          lockNodes,
          lockType);
    }
  }

  /** Unlock the tree lock. */
  public void unlock() {
    if (lockType == null) {
      throw new IllegalStateException("We must lock the tree lock before unlock it.");
    }

    while (!heldLocks.isEmpty()) {
      Pair<TreeLockNode, LockType> pair = heldLocks.pop();
      TreeLockNode current = pair.getLeft();
      LockType type = pair.getRight();
      current.unlock(type);
      if (LOG.isTraceEnabled()) {
        LOG.trace("Unlocked node: {}, lock type: {}", current, type);
      }
    }

    if (LOG.isTraceEnabled()) {
      LOG.trace(
          "Unlocked the tree lock, identifier: {}, lockNodes: [{}], lock type: {}",
          identifier,
          lockNodes,
          lockType);
    }
  }
}
