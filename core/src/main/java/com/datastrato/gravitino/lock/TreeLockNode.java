/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.lock;

import com.datastrato.gravitino.NameIdentifier;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class TreeLockNode {
  public static final Logger LOG = LoggerFactory.getLogger(TreeLockNode.class);
  private final NameIdentifier ident;
  private final ReentrantReadWriteLock readWriteLock;
  private final Map<NameIdentifier, TreeLockNode> childMap;

  private final AtomicLong referenceCount = new AtomicLong();
  private long lastAccessTime;

  public TreeLockNode(NameIdentifier identifier) {
    this.ident = identifier;
    this.readWriteLock = new ReentrantReadWriteLock();
    this.childMap = new ConcurrentHashMap<>();
  }

  public long getLastAccessTime() {
    return lastAccessTime;
  }

  public NameIdentifier getIdent() {
    return ident;
  }

  /**
   * Try to lock the node with the given lock type.
   *
   * @param lockType The lock type to lock the node.
   * @return True if the node is locked successfully, false otherwise.
   */
  public boolean tryLock(LockType lockType) {
    boolean success =
        lockType == LockType.READ
            ? readWriteLock.readLock().tryLock()
            : readWriteLock.writeLock().tryLock();
    if (success) {
      referenceCount.getAndIncrement();

      LOG.info(
          "Node {} has been lock with '{}' lock, reference count = {}",
          ident,
          lockType,
          referenceCount.get());
    }

    return success;
  }

  /**
   * Lock the node with the given lock type. This method should be followed by {@link
   * #unlock(LockType)}.
   *
   * @param lockType The lock type to lock the node.
   */
  public void lock(LockType lockType) {
    referenceCount.getAndIncrement();
    if (lockType == LockType.READ) {
      readWriteLock.readLock().lock();
    } else {
      readWriteLock.writeLock().lock();
    }

    LOG.info(
        "Node {} has been lock with '{}' lock, reference count = {}",
        ident,
        lockType,
        referenceCount.get());
  }

  /**
   * Unlock the node with the given lock type. This method should be called after {@link
   * #lock(LockType)}, and the lock type should be the same as the lock type in {@link
   * #lock(LockType)}.
   *
   * @param lockType The lock type to unlock the node.
   */
  public void unlock(LockType lockType) {
    referenceCount.getAndDecrement();
    lastAccessTime = System.currentTimeMillis();
    if (lockType == LockType.READ) {
      readWriteLock.readLock().unlock();
    } else {
      readWriteLock.writeLock().unlock();
    }
    LOG.info(
        "Node {} has been unlock with '{}' lock, reference count = {}",
        ident,
        lockType,
        referenceCount.get());
  }

  /**
   * Get the tree lock node by the given name identifier. If the node doesn't exist, create a new
   * TreeNode.
   *
   * @param ident The name identifier of a resource such as entity or others.
   * @return The tree lock node of this ident.
   */
  public TreeLockNode getOrCreateChild(NameIdentifier ident) {
    TreeLockNode treeNode = childMap.get(ident);
    lastAccessTime = System.currentTimeMillis();
    if (treeNode == null) {
      synchronized (this) {
        treeNode = childMap.get(ident);
        if (treeNode == null) {
          treeNode = new TreeLockNode(ident);
          childMap.put(ident, treeNode);
        }
      }
    }

    return treeNode;
  }

  public synchronized List<TreeLockNode> getAllChildren() {
    return Lists.newArrayList(childMap.values());
  }

  public synchronized void removeChild(NameIdentifier name) {
    childMap.remove(name);
  }
}
