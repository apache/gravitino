/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.lock;

import static com.datastrato.gravitino.lock.TreeLockConfigs.TREE_LOCK_CLEAN_INTERVAL;
import static com.datastrato.gravitino.lock.TreeLockConfigs.TREE_LOCK_MAX_NODE_IN_MEMORY;
import static com.datastrato.gravitino.lock.TreeLockConfigs.TREE_LOCK_MIN_NODE_IN_MEMORY;

import com.datastrato.gravitino.Config;
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

  static final NameIdentifier ROOT = NameIdentifier.of("/");

  @VisibleForTesting final TreeLockNode treeLockRootNode;
  final AtomicLong totalNodeCount = new AtomicLong(1);

  // The maximum number of tree lock nodes to keep in memory. If the total node count is greater
  // than this value, we will do the cleanup.
  @VisibleForTesting long maxTreeNodeInMemory;
  // If the total node count is less than this value, we will not do the cleanup.
  @VisibleForTesting long minTreeNodeInMemory;

  // The interval in seconds to clean up the stale tree lock nodes.
  @VisibleForTesting long cleanTreeNodeIntervalInSecs;

  private void initParameters(Config config) {
    long maxNodesInMemory = config.get(TREE_LOCK_MAX_NODE_IN_MEMORY);
    if (maxNodesInMemory <= 0) {
      throw new IllegalArgumentException(
          String.format(
              "The maximum number of tree lock nodes '%d' should be greater than 0",
              maxNodesInMemory));
    }

    long minNodesInMemory = config.get(TREE_LOCK_MIN_NODE_IN_MEMORY);
    if (minNodesInMemory <= 0) {
      throw new IllegalArgumentException(
          String.format(
              "The minimum number of tree lock nodes '%d' should be greater than 0",
              minNodesInMemory));
    }

    if (maxNodesInMemory <= minNodesInMemory) {
      throw new IllegalArgumentException(
          String.format(
              "The maximum number of tree lock nodes '%d' should be greater than the minimum number of tree lock nodes '%d'",
              maxNodesInMemory, minNodesInMemory));
    }
    this.maxTreeNodeInMemory = maxNodesInMemory;
    this.minTreeNodeInMemory = minNodesInMemory;

    long cleanIntervalInSecs = config.get(TREE_LOCK_CLEAN_INTERVAL);
    if (cleanIntervalInSecs <= 0) {
      throw new IllegalArgumentException(
          String.format(
              "The interval in seconds to clean up the stale tree lock nodes '%d' should be greater than 0",
              cleanIntervalInSecs));
    }

    this.cleanTreeNodeIntervalInSecs = cleanIntervalInSecs;
  }

  private void startNodeCleaner() {
    ScheduledThreadPoolExecutor lockCleaner =
        new ScheduledThreadPoolExecutor(
            1,
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("tree-lock-cleaner-%d")
                .build());

    lockCleaner.scheduleAtFixedRate(
        () -> {
          long nodeCount = totalNodeCount.get();
          LOG.trace("Total tree lock node count: {}", nodeCount);
          if (nodeCount > maxTreeNodeInMemory) {
            treeLockRootNode
                .getAllChildren()
                .forEach(child -> evictStaleNodes(child, treeLockRootNode));
          }
        },
        cleanTreeNodeIntervalInSecs,
        cleanTreeNodeIntervalInSecs,
        TimeUnit.SECONDS);
  }

  public LockManager(Config config) {
    treeLockRootNode = new TreeLockNode(ROOT.name(), this);

    // Init the parameters.
    initParameters(config);

    // Start tree lock cleaner.
    startNodeCleaner();
  }

  /**
   * Evict the stale nodes from the tree lock node.
   *
   * @param treeNode The tree lock node to evict.
   * @param parent The parent of the tree lock node.
   */
  @VisibleForTesting
  void evictStaleNodes(TreeLockNode treeNode, TreeLockNode parent) {
    // We will not evict the node tree if the total node count is less than the
    // MIN_TREE_NODE_IN_MEMORY.
    // Do not need to consider thread-safe issues.
    if (totalNodeCount.get() < minTreeNodeInMemory) {
      return;
    }

    // Handle from leaf nodes first.
    treeNode.getAllChildren().forEach(child -> evictStaleNodes(child, treeNode));

    // Handle self node.
    if (treeNode.getReferenceCount() == 0) {
      synchronized (parent) {
        // Once goes here, the parent node has been locked, so the reference could not be changed.
        if (treeNode.getReferenceCount() == 0) {
          parent.removeChild(treeNode.getName());
          long leftNodeCount = totalNodeCount.decrementAndGet();
          LOG.trace(
              "Evict stale tree lock node '{}', current left nodes '{}'",
              treeNode.getName(),
              leftNodeCount);
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
    List<TreeLockNode> treeLockNodes = Lists.newArrayList();

    try {
      TreeLockNode lockNode = treeLockRootNode;
      lockNode.addReference();
      treeLockNodes.add(lockNode);

      if (identifier == ROOT) {
        // The lock tree root node
        return new TreeLock(treeLockNodes);
      }

      String[] levels = identifier.namespace().levels();
      levels = ArrayUtils.add(levels, identifier.name());

      TreeLockNode child;
      for (int i = 0; i < levels.length; i++) {
        synchronized (lockNode) {
          child = lockNode.getOrCreateChild(levels[i]);
        }
        treeLockNodes.add(child);
        lockNode = child;
      }

      return new TreeLock(treeLockNodes);
    } catch (Exception e) {
      LOG.error("Failed to create tree lock {}", identifier, e);
      // Release reference if fails.
      for (TreeLockNode node : treeLockNodes) {
        node.decReference();
      }

      throw e;
    }
  }
}
