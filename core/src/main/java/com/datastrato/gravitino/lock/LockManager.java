/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.lock;

import com.datastrato.gravitino.NameIdentifier;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import java.util.Map;
import java.util.Stack;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LockManager {
  public static final Logger LOG = LoggerFactory.getLogger(LockManager.class);

  private static final NameIdentifier ROOT = NameIdentifier.ROOT;
  private final Map<NameIdentifier, LockNode> lockNodeMap = Maps.newConcurrentMap();
  private final ThreadLocal<Stack<LockObject>> currentLocked = ThreadLocal.withInitial(Stack::new);

  public LockManager() {
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
    LockNode lockNode = lockNodeMap.get(identifier);
    if (lockNode == null) {
      synchronized (this) {
        lockNode = lockNodeMap.get(identifier);
        if (lockNode == null) {
          lockNode = new LockNode();
          lockNodeMap.put(identifier, lockNode);
        }
      }
    }
    return lockNode;
  }

  public void lockResourcePath(NameIdentifier identifier, LockType lockType) {
    Stack<NameIdentifier> parents = getResourcePath(identifier);

    try {
      // Lock parent with read lock
      while (!parents.isEmpty()) {
        NameIdentifier parent = parents.pop();
        LockNode lockNode = getOrCreateLockNode(parent);
        lockNode.lock(LockType.READ);
        addLock(lockNode, LockType.READ);
      }

      // Lock self with the value of `lockType`
      LockNode node = getOrCreateLockNode(identifier);
      node.lock(lockType);
      addLock(node, lockType);
    } catch (Exception e) {
      // Unlock those that have been locked.
      rollbackLocks();
      LOG.error("Failed to lock resource path {}", identifier, e);
      throw e;
    }
  }

  public void unlockResourcePath(LockType lockType) {
    Stack<LockObject> stack = currentLocked.get();
    stack.pop().lockNode.release(lockType);

    while (!stack.isEmpty()) {
      LockObject lockObject = stack.pop();
      lockObject.lockNode.release(lockObject.lockType);
    }
  }

  private void rollbackLocks() {
    Stack<LockObject> locks = currentLocked.get();
    while (!locks.isEmpty()) {
      LockObject lockObject = locks.pop();
      lockObject.lockNode.release(lockObject.lockType);
    }
  }

  private void addLock(LockNode lockNode, LockType lockType) {
    currentLocked.get().push(LockObject.of(lockNode, lockType));
  }
}
