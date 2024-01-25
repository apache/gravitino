/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.lock;

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
  private final String name;
  private final ReentrantReadWriteLock readWriteLock;
  private final Map<String, TreeLockNode> childMap;

  private final AtomicLong referenceCount = new AtomicLong();
  private long lastAccessTime;

  public TreeLockNode(String name) {
    this.name = name;
    this.readWriteLock = new ReentrantReadWriteLock();
    this.childMap = new ConcurrentHashMap<>();
  }

  public long getLastAccessTime() {
    return lastAccessTime;
  }

  public String getName() {
    return name;
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
        name,
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
        name,
        lockType,
        referenceCount.get());
  }

  /**
   * Get the tree lock node of resource 'name', it's the last part of the resource path.
   *
   * @param name The name of the resource.
   * @return The tree lock node of the resource.
   */
  public TreeLockNode getOrCreateChild(String name) {
    TreeLockNode treeNode = childMap.get(name);
    lastAccessTime = System.currentTimeMillis();
    if (treeNode == null) {
      synchronized (this) {
        treeNode = childMap.get(name);
        if (treeNode == null) {
          treeNode = new TreeLockNode(name);
          childMap.put(name, treeNode);
        }
      }
    }

    return treeNode;
  }

  public List<TreeLockNode> getAllChildren() {
    return Lists.newArrayList(childMap.values());
  }

  public synchronized void removeChild(String name) {
    childMap.remove(name);
  }
}
