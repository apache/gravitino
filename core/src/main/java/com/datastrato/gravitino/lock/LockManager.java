/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.lock;

import static com.datastrato.gravitino.Configs.TREE_LOCK_CLEAN_INTERVAL;
import static com.datastrato.gravitino.Configs.TREE_LOCK_MAX_NODE_IN_MEMORY;
import static com.datastrato.gravitino.Configs.TREE_LOCK_MIN_NODE_IN_MEMORY;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.NameIdentifier;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * LockManager is a lock manager that manages the tree locks. It will serve as a factory to create
 * the tree lock and do the cleanup for the stale tree lock nodes. For more, please refer to {@link
 * TreeLock} and {@link TreeLockNode}.
 *
 * <p>It has two main functions: 1. Create the tree lock. 2. Clean up the stale tree lock nodes
 * shared by all tree lock instances.
 */
public class LockManager {
  private static final Logger LOG = LoggerFactory.getLogger(LockManager.class);

  static final NameIdentifier ROOT = NameIdentifier.of("/");

  @VisibleForTesting TreeLockNode treeLockRootNode;
  final AtomicLong totalNodeCount = new AtomicLong(1);

  // The maximum number of tree lock nodes to keep in memory. If the total node count is greater
  // than this value, we will do the cleanup.
  long maxTreeNodeInMemory;
  // If the total node count is less than this value, we will not do the cleanup.
  @VisibleForTesting long minTreeNodeInMemory;

  // The interval in seconds to clean up the stale tree lock nodes.
  @VisibleForTesting long cleanTreeNodeIntervalInSecs;

  private void initParameters(Config config) {
    long maxNodesInMemory = config.get(TREE_LOCK_MAX_NODE_IN_MEMORY);
    Preconditions.checkArgument(
        maxNodesInMemory > 0,
        String.format(
            "The maximum number of tree lock nodes '%d' should be greater than 0",
            maxNodesInMemory));

    long minNodesInMemory = config.get(TREE_LOCK_MIN_NODE_IN_MEMORY);
    Preconditions.checkArgument(
        minNodesInMemory > 0,
        String.format(
            "The minimum number of tree lock nodes '%d' should be greater than 0",
            minNodesInMemory));

    Preconditions.checkArgument(
        maxNodesInMemory > minNodesInMemory,
        String.format(
            "The maximum number of tree lock nodes '%d' should be greater than the minimum number of tree lock nodes '%d'",
            maxNodesInMemory, minNodesInMemory));
    this.maxTreeNodeInMemory = maxNodesInMemory;
    this.minTreeNodeInMemory = minNodesInMemory;

    long cleanIntervalInSecs = config.get(TREE_LOCK_CLEAN_INTERVAL);
    Preconditions.checkArgument(
        cleanIntervalInSecs > 0,
        String.format(
            "The interval in seconds to clean up the stale tree lock nodes '%d' should be greater than 0",
            cleanIntervalInSecs));

    this.cleanTreeNodeIntervalInSecs = cleanIntervalInSecs;
  }

  private void startDeadLockChecker() {
    ScheduledThreadPoolExecutor deadLockChecker =
        new ScheduledThreadPoolExecutor(
            1,
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("tree-lock-dead-lock-checker-%d")
                .build());

    deadLockChecker.scheduleAtFixedRate(
        () -> {
          LOG.info("Start to check the dead lock...");
          checkDeadLock(treeLockRootNode);
          LOG.info("Finish to check the dead lock...");
        },
        0,
        60,
        TimeUnit.SECONDS);
  }

  /**
   * Check the deadlock for the given root node.
   *
   * @param node The root node to check.
   */
  void checkDeadLock(TreeLockNode node) {
    // Check child first
    node.getAllChildren().forEach(this::checkDeadLock);

    // Check self
    node.getHoldingThreadTimestamp()
        .forEach(
            (thread, ts) -> {
              // If the thread is holding the lock for more than 30 seconds, we will log it.
              if (System.currentTimeMillis() - ts > 30000) {
                LOG.warn(
                    "Dead lock detected for thread {} on node {}, threads that holding the node: {} ",
                    thread,
                    node,
                    node.getHoldingThreadTimestamp());
              }
            });
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
          LOG.info("Total tree lock node count: {}", nodeCount);
          // If the total node count is greater than the maxTreeNodeInMemory * 0.5, we will do the
          // clear up in case of the memory explosion.
          if (nodeCount > maxTreeNodeInMemory * 0.5) {
            StopWatch watch = StopWatch.createStarted();
            LOG.trace("Start to clean up the stale tree lock nodes...");
            treeLockRootNode
                .getAllChildren()
                .forEach(child -> evictStaleNodes(child, treeLockRootNode));
            LOG.info(
                "Finish to clean up the stale tree lock nodes, cost: {}, after clean node count: {}",
                watch.getTime(),
                totalNodeCount.get());
          }
        },
        cleanTreeNodeIntervalInSecs,
        cleanTreeNodeIntervalInSecs,
        TimeUnit.SECONDS);
  }

  public LockManager(Config config) {
    treeLockRootNode = new TreeLockNode(ROOT.name());

    // Init the parameters.
    initParameters(config);

    // Start tree lock cleaner.
    startNodeCleaner();

    // Start deadlock checker.
    startDeadLockChecker();
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
    if (treeNode.getReference() == 0) {
      synchronized (parent) {
        // Once goes here, the parent node has been locked, so the reference of child (treeNode)
        // could not be changed.
        if (treeNode.getReference() == 0) {
          parent.removeChild(treeNode.getName());
          long leftNodeCount = totalNodeCount.decrementAndGet();
          if (LOG.isTraceEnabled()) {
            LOG.trace(
                "Evict stale tree lock node '{}', current left nodes '{}'",
                treeNode.getName(),
                leftNodeCount);
          }
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
    checkTreeNodeIsFull();

    List<TreeLockNode> treeLockNodes = Lists.newArrayList();
    try {
      TreeLockNode lockNode = treeLockRootNode;
      lockNode.addReference();
      treeLockNodes.add(lockNode);

      if (identifier == ROOT) {
        // The lock tree root node
        return new TreeLock(treeLockNodes, identifier);
      }

      String[] levels = identifier.namespace().levels();
      levels = ArrayUtils.add(levels, identifier.name());

      TreeLockNode child;
      for (String level : levels) {
        synchronized (lockNode) {
          Pair<TreeLockNode, Boolean> pair = lockNode.getOrCreateChild(level);
          child = pair.getKey();
          // If the child node is newly created, we should increase the total node counts.
          if (pair.getValue()) {
            totalNodeCount.incrementAndGet();
          }
        }
        treeLockNodes.add(child);
        lockNode = child;
      }

      return new TreeLock(treeLockNodes, identifier);
    } catch (Exception e) {
      LOG.error("Failed to create tree lock {}", identifier, e);
      // Release reference if fails.
      for (TreeLockNode node : treeLockNodes) {
        node.decReference();
      }

      throw e;
    }
  }

  /**
   * Check if the total node count is greater than the maxTreeNodeInMemory, if so, we should throw
   * an exception.
   */
  private void checkTreeNodeIsFull() {
    // If the total node count is greater than the max node counts, in case of memory
    // leak and explosion, we should throw an exception.
    long currentNodeCount = totalNodeCount.get();
    if (currentNodeCount > maxTreeNodeInMemory) {
      throw new IllegalStateException(
          "The total node count '"
              + currentNodeCount
              + "' has reached the max node count '"
              + maxTreeNodeInMemory
              + "', please increase the max node count or wait for a while to avoid the performance issue.");
    }
  }
}
