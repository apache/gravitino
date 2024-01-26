/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.lock;

import com.datastrato.gravitino.NameIdentifier;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * LockManager is a lock manager that manages the tree locks. It will service as a factory to create
 * the tree lock and do the cleanup for the stale tree locks node.
 */
public class LockManager {
  private static final Logger LOG = LoggerFactory.getLogger(LockManager.class);

  @VisibleForTesting final TreeLockNode treeLockRootNode = new TreeLockNode(NameIdentifier.ROOT);
  private final ScheduledThreadPoolExecutor lockCleaner;

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
                .forEach(child -> evictStaleNodes(child, treeLockRootNode)),
        10,
        30,
        TimeUnit.SECONDS);
  }

  /**
   * Evict the stale nodes from the tree lock node if the last access time is earlier than the given
   * time.
   *
   * @param treeNode The tree lock node to evict.
   * @param parent The parent of the tree lock node.
   */
  @VisibleForTesting
  void evictStaleNodes(TreeLockNode treeNode, TreeLockNode parent) {
    if (treeNode.getReferenceCount() == 0) {
      synchronized (treeNode) {
        // Once goes here, the node is locked, so the reference could not be increased(please see
        // TreeLockNode#addReference)
        // thus No TreeLock will use (refer to) the TreeLockNode, so it's safe to remove the node.
        if (treeNode.getReferenceCount() == 0) {
          parent.removeChild(treeNode.getIdent());
          LOG.info("Evict stale tree lock node {}", treeNode.getIdent());
        } else {
          treeNode.getAllChildren().forEach(child -> evictStaleNodes(child, treeNode));
        }
      }
    } else {
      treeNode.getAllChildren().forEach(child -> evictStaleNodes(child, treeNode));
    }
  }

  /**
   * Create a tree lock with the given identifier.
   *
   * @param identifier The identifier of the tree lock.
   * @return The created tree lock.
   */
  public TreeLock createTreeLock(NameIdentifier identifier) {
    TreeLockNode lockNode = treeLockRootNode;
    treeLockRootNode.addReference();

    List<TreeLockNode> treeLockNodes = Lists.newArrayList(lockNode);
    if (identifier.equals(NameIdentifier.ROOT)) {
      // The lock tree root node
      return new TreeLock(treeLockNodes);
    }

    String[] levels = identifier.namespace().levels();
    levels = ArrayUtils.add(levels, identifier.name());

    try {
      for (int i = 0; i < levels.length; i++) {
        NameIdentifier ident = NameIdentifier.of(ArrayUtils.subarray(levels, 0, i + 1));
        lockNode = lockNode.getOrCreateChild(ident);
        lockNode.addReference();
        treeLockNodes.add(lockNode);
      }
    } catch (Exception e) {
      LOG.error("Failed to create tree lock {}", identifier, e);
      throw e;
    }

    return new TreeLock(treeLockNodes);
  }
}
