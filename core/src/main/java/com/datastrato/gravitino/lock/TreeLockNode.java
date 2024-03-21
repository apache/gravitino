/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.lock;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TreeLockNode is a node in the tree lock; all tree lock nodes will be assembled to a tree
 * structure, which corresponds to the resource path like name identifier space.
 *
 * <p>Each node will have a read-write lock to protect the node. The node will also have a map to
 * store the children. For more, please refer to {@link TreeLock}.
 */
public class TreeLockNode {
  public static final Logger LOG = LoggerFactory.getLogger(TreeLockNode.class);
  private final String name;
  private final ReentrantReadWriteLock readWriteLock;
  @VisibleForTesting final Map<String, TreeLockNode> childMap;
  private final Map<Thread, Long> holdingThreadTimestamp = new ConcurrentHashMap<>();

  // The reference count of this node. The reference count is used to track the number of the
  // TreeLocks that are using this node. If the reference count is 0, it means that no TreeLock is
  // using this node, and this node can be removed from the tree.
  private final AtomicLong referenceCount = new AtomicLong();

  protected TreeLockNode(String name) {
    this.name = name;
    this.readWriteLock = new ReentrantReadWriteLock();
    this.childMap = new ConcurrentHashMap<>();
  }

  public String getName() {
    return name;
  }

  Map<Thread, Long> getHoldingThreadTimestamp() {
    return holdingThreadTimestamp;
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
  void lock(LockType lockType) {
    if (lockType == LockType.READ) {
      readWriteLock.readLock().lock();
    } else {
      readWriteLock.writeLock().lock();
    }

    holdingThreadTimestamp.put(Thread.currentThread(), System.currentTimeMillis());
    if (LOG.isTraceEnabled()) {
      LOG.trace(
          "Node {} has been lock with '{}' lock, hold by {} at {}, current holding threads: {}",
          this,
          lockType,
          Thread.currentThread(),
          System.currentTimeMillis(),
          holdingThreadTimestamp);
    }
  }

  /**
   * Unlock the node with the given lock type. This method should be called after {@link
   * #lock(LockType)}, and the lock type should be the same as the lock type in {@link
   * #lock(LockType)}.
   *
   * @param lockType The lock type to unlock the node.
   */
  void unlock(LockType lockType) {
    if (lockType == LockType.READ) {
      readWriteLock.readLock().unlock();
    } else {
      readWriteLock.writeLock().unlock();
    }

    this.referenceCount.decrementAndGet();

    long holdStartTime = holdingThreadTimestamp.remove(Thread.currentThread());
    if (LOG.isTraceEnabled()) {
      LOG.trace(
          "Node {} has been unlock with '{}' lock, hold by {} for {} ms, current holding threads: {}",
          this,
          lockType,
          Thread.currentThread(),
          System.currentTimeMillis() - holdStartTime,
          holdingThreadTimestamp);
    }
  }

  /**
   * Get the tree lock node by the given name. If the node doesn't exist, create a new TreeNode.
   *
   * <p>Note: This method should always be guarded by object lock.
   *
   * @param name The name of a resource such as entity or others.
   * @return A pair of the tree lock node and a boolean value indicating whether the node is newly
   *     created.
   */
  Pair<TreeLockNode, Boolean> getOrCreateChild(String name) {
    boolean[] newCreated = new boolean[] {false};
    TreeLockNode childNode =
        childMap.computeIfAbsent(
            name,
            k -> {
              TreeLockNode newNode = new TreeLockNode(name);
              if (LOG.isTraceEnabled()) {
                LOG.trace("Create tree lock node '{}' as a child of '{}'", name, this.name);
              }
              newCreated[0] = true;
              return newNode;
            });

    childNode.addReference();
    return Pair.of(childNode, newCreated[0]);
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
    sb.append("ident=").append(name).append(",");
    sb.append("hashCode=").append(hashCode());
    sb.append('}');
    return sb.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TreeLockNode)) {
      return false;
    }
    TreeLockNode that = (TreeLockNode) o;
    return Objects.equal(name, that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(name);
  }
}
