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

@ThreadSafe
public class TreeLockNode {
  private final String name;
  private final ReentrantReadWriteLock reentrantReadWriteLock;
  private final Map<String, TreeLockNode> children;

  private final AtomicLong referenceCount = new AtomicLong();
  private long lastAccessTime;

  public TreeLockNode(String name) {
    this.name = name;
    this.reentrantReadWriteLock = new ReentrantReadWriteLock();
    this.children = new ConcurrentHashMap<>();
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
      reentrantReadWriteLock.readLock().lock();
    } else {
      reentrantReadWriteLock.writeLock().lock();
    }
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
      reentrantReadWriteLock.readLock().unlock();
    } else {
      reentrantReadWriteLock.writeLock().unlock();
    }
  }

  /**
   * Get the tree lock node of resource 'name', it's the last part of the resource path.
   *
   * @param name The name of the resource.
   * @return The tree lock node of the resource.
   */
  public TreeLockNode getOrCreateChild(String name) {
    TreeLockNode treeNode = children.get(name);
    lastAccessTime = System.currentTimeMillis();
    if (treeNode == null) {
      synchronized (this) {
        treeNode = children.get(name);
        if (treeNode == null) {
          treeNode = new TreeLockNode(name);
          children.put(name, treeNode);
        }
      }
    }

    return treeNode;
  }

  public List<TreeLockNode> getAllChildren() {
    return Lists.newArrayList(children.values());
  }

  public synchronized void removeChild(String name) {
    children.remove(name);
  }
}
