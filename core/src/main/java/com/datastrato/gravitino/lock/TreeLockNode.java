/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.lock;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import java.util.Collections;
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
  @VisibleForTesting final Map<String, TreeLockNode> childMap;
  private final LockManager lockManager;

  // The reference count of this node. The reference count is used to track the number of the
  // TreeLocks that are using this node. If the reference count is 0, it means that no TreeLock is
  // using this node, and this node can be removed from the tree.
  private final AtomicLong referenceCount = new AtomicLong();

  public TreeLockNode(String name, LockManager manager) {
    this.name = name;
    this.readWriteLock = new ReentrantReadWriteLock();
    this.childMap = new ConcurrentHashMap<>();
    this.lockManager = manager;
  }

  public String getName() {
    return name;
  }

  /**
   * Increase the reference count of this node. The reference count should always be greater than or
   * equal to 0.
   */
  synchronized void addReference() {
    referenceCount.getAndIncrement();
  }

  /**
   * Decrease the reference count of this node. The reference count should always be greater than or
   * equal to 0.
   */
  synchronized void decReference() {
    referenceCount.getAndDecrement();
  }

  long getReference() {
    return referenceCount.get();
  }

  /**
   * Lock the node with the given lock type. This method should be followed by {@link
   * #unlock(LockType)}.
   *
   * @param lockType The lock type to lock the node.
   */
  public void lock(LockType lockType) {
    if (lockType == LockType.READ) {
      readWriteLock.readLock().lock();
    } else {
      readWriteLock.writeLock().lock();
    }

    LOG.trace("Node {} has been lock with '{}' lock", name, lockType);
  }

  /**
   * Unlock the node with the given lock type. This method should be called after {@link
   * #lock(LockType)}, and the lock type should be the same as the lock type in {@link
   * #lock(LockType)}.
   *
   * @param lockType The lock type to unlock the node.
   */
  public void unlock(LockType lockType) {
    if (lockType == LockType.READ) {
      readWriteLock.readLock().unlock();
    } else {
      readWriteLock.writeLock().unlock();
    }

    LOG.trace("Node {} has been unlock with '{}' lock", name, lockType);
  }

  /**
   * Get the tree lock node by the given name. If the node doesn't exist, create a new TreeNode.
   *
   * <p>Note: This method should always be guarded by object lock.
   *
   * @param name The name of a resource such as entity or others.
   * @return The tree lock node of this ident.
   */
  TreeLockNode getOrCreateChild(String name) {
    TreeLockNode childNode =
        childMap.computeIfAbsent(
            name,
            k -> {
              TreeLockNode newNode = new TreeLockNode(name, lockManager);
              lockManager.totalNodeCount.getAndIncrement();
              LOG.trace("Create tree lock node '{}' as a child of '{}'", name, this.name);
              return newNode;
            });

    childNode.addReference();
    return childNode;
  }

  /**
   * Get all the children of this node. The returned list is unmodifiable and the order is random.
   * The reason why we return a random order list is that we want to avoid the cases that the first
   * child is always to be dropped firstly and the last child is always the last to be dropped,
   * chances are that the first child is always the most popular one and the last child is always
   * the least popular.
   *
   * @return The list of all the children of this node.
   */
  synchronized List<TreeLockNode> getAllChildren() {
    List<TreeLockNode> children = Lists.newArrayList(childMap.values());
    Collections.shuffle(children);
    return Collections.unmodifiableList(children);
  }

  /**
   * Remove the child node by the given name identifier.
   *
   * <p>Note: This method should be guarded by object lock.
   *
   * @param name The name of a resource such as entity or others.
   */
  void removeChild(String name) {
    childMap.remove(name);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("TreeLockNode{");
    sb.append("ident=").append(name);
    sb.append('}');
    return sb.toString();
  }
}
