/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.lock;

import com.datastrato.gravitino.NameIdentifier;
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
  private final NameIdentifier ident;
  private final ReentrantReadWriteLock readWriteLock;
  @VisibleForTesting final Map<NameIdentifier, TreeLockNode> childMap;
  private final LockManager lockManager;

  private final AtomicLong referenceCount = new AtomicLong();

  public TreeLockNode(NameIdentifier identifier, LockManager manager) {
    this.ident = identifier;
    this.readWriteLock = new ReentrantReadWriteLock();
    this.childMap = new ConcurrentHashMap<>();
    this.lockManager = manager;
  }

  public NameIdentifier getIdent() {
    return ident;
  }

  // Why is this method synchronized, please see the comment in the method
  // LockManager#evictStaleNodes
  synchronized void addReference() {
    referenceCount.getAndIncrement();
  }

  synchronized void decReference() {
    referenceCount.getAndDecrement();
  }

  long getReferenceCount() {
    return referenceCount.get();
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
    if (lockType == LockType.READ) {
      readWriteLock.readLock().lock();
    } else {
      readWriteLock.writeLock().lock();
    }

    LOG.trace("Node {} has been lock with '{}' lock", ident, lockType);
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

    LOG.trace("Node {} has been unlock with '{}' lock", ident, lockType);
  }

  /**
   * Get the tree lock node by the given name identifier. If the node doesn't exist, create a new
   * TreeNode.
   *
   * <p>Note: This method should always be guarded by object lock.
   *
   * @param ident The name identifier of a resource such as entity or others.
   * @return The tree lock node of this ident.
   */
  TreeLockNode getOrCreateChild(NameIdentifier ident) {
    TreeLockNode childNode = childMap.get(ident);
    if (childNode == null) {
      childNode = new TreeLockNode(ident, lockManager);
      lockManager.totalNodeCount.getAndIncrement();
      childMap.put(ident, childNode);

      LOG.trace("Create tree lock node '{}' as a child of '{}'", ident, this.ident);
    }

    childNode.addReference();
    return childNode;
  }

  /**
   * Get all the children of this node. The returned list is unmodifiable and the order is random.
   *
   * @return The list of all the children of this node.
   */
  synchronized List<TreeLockNode> getAllChildren() {
    List<TreeLockNode> children = Lists.newArrayList(childMap.values());
    Collections.shuffle(children);
    return Collections.unmodifiableList(children);
  }

  void removeChild(NameIdentifier name) {
    childMap.remove(name);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("TreeLockNode{");
    sb.append("ident=").append(ident);
    sb.append('}');
    return sb.toString();
  }
}
