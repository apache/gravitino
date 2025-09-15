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

package org.apache.gravitino.lock;

import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.gravitino.NameIdentifier;
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

        treeLockNode.addHoldingThreadTimestamp(
            Thread.currentThread(), identifier, System.currentTimeMillis());
        if (LOG.isTraceEnabled()) {
          LOG.trace(
              "Node {} has been lock with '{}' lock, hold by {} with ident '{}' at {}",
              this,
              type,
              Thread.currentThread(),
              identifier,
              System.currentTimeMillis());
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
    if (heldLocks.isEmpty()) {
      throw new IllegalStateException("We must hold a lock before unlocking it.");
    }

    while (!heldLocks.isEmpty()) {
      Pair<TreeLockNode, LockType> pair = heldLocks.pop();
      TreeLockNode current = pair.getLeft();
      LockType type = pair.getRight();
      current.unlock(type);

      long holdStartTime = current.removeHoldingThreadTimestamp(Thread.currentThread(), identifier);
      if (LOG.isTraceEnabled()) {
        LOG.trace(
            "Node {} has been unlock with '{}' lock, hold by {} with ident '{}' for {} ms",
            this,
            type,
            Thread.currentThread(),
            identifier,
            System.currentTimeMillis() - holdStartTime);
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
