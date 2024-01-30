/*
 * Copyright 2024 Datastrato Pvt Ltd.
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
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * LockManager is a lock manager that manages the tree locks. It will service as a factory to create
 * the tree lock and do the cleanup for the stale tree locks node.
 */
public class LockManager {
  private static final Logger LOG = LoggerFactory.getLogger(LockManager.class);
  @VisibleForTesting final TreeLockNode treeLockRootNode;
  final AtomicLong totalNodeCount = new AtomicLong(1);

  // TODO (yuqi) make this configurable
  private static final long MAX_TREE_NODE_IN_MEMORY = 10000L;

  private final ScheduledThreadPoolExecutor lockCleaner;

  public LockManager() {
    treeLockRootNode = new TreeLockNode(NameIdentifier.ofRoot(), this);
    this.lockCleaner =
        new ScheduledThreadPoolExecutor(
            1,
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("tree-lock-cleaner-%d")
                .build());

    lockCleaner.scheduleAtFixedRate(
        () -> {
          long nodeCount = totalNodeCount.get();
          LOG.info("Total tree lock node count: {}", nodeCount);
          if (nodeCount > MAX_TREE_NODE_IN_MEMORY) {
            treeLockRootNode
                .getAllChildren()
                .forEach(child -> evictStaleNodes(child, treeLockRootNode));
          }
        },
        120,
        5,
        TimeUnit.SECONDS);
  }

  /**
   * Evict the stale nodes from the tree lock node.
   *
   * @param treeNode The tree lock node to evict.
   * @param parent The parent of the tree lock node.
   */
  @VisibleForTesting
  void evictStaleNodes(TreeLockNode treeNode, TreeLockNode parent) {
    // Handle from leaf nodes first.
    treeNode.getAllChildren().forEach(child -> evictStaleNodes(child, treeNode));

    // Handle self node.
    if (treeNode.getReferenceCount() == 0) {
      synchronized (parent) {
        // Once goes here, the parent node has been locked, so the reference could not be changed.
        if (treeNode.getReferenceCount() == 0) {
          parent.removeChild(treeNode.getIdent());
          totalNodeCount.decrementAndGet();
          LOG.info("Evict stale tree lock node '{}' and all its children", treeNode.getIdent());
        } else {
          treeNode.getAllChildren().forEach(child -> evictStaleNodes(child, treeNode));
        }
      }
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
    if (identifier.equals(NameIdentifier.ofRoot())) {
      // The lock tree root node
      return new TreeLock(treeLockNodes);
    }

    String[] levels = identifier.namespace().levels();
    levels = ArrayUtils.add(levels, identifier.name());

    try {
      for (int i = 0; i < levels.length; i++) {
        NameIdentifier ident = NameIdentifier.of(ArrayUtils.subarray(levels, 0, i + 1));
        lockNode = lockNode.getOrCreateChild(ident);
        treeLockNodes.add(lockNode);
      }
    } catch (Exception e) {
      LOG.error("Failed to create tree lock {}", identifier, e);
      throw e;
    }

    return new TreeLock(treeLockNodes);
  }
}
