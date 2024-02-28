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
import com.datastrato.gravitino.Namespace;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletionService;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

public class TestLockManager {
  private static final String[] ENTITY_NAMES = {
    "entity1",
    "entity2",
    "entity3",
    "entity4",
    "entity5",
    "entity6",
    "entity7",
    "entity8",
    "entity9",
    "entity10"
  };

  private static String getName(int i) {
    return "entity_" + i;
  }

  static Config getConfig() {
    Config config = Mockito.mock(Config.class);
    Mockito.when(config.get(TREE_LOCK_MAX_NODE_IN_MEMORY)).thenReturn(100000L);
    Mockito.when(config.get(TREE_LOCK_MIN_NODE_IN_MEMORY)).thenReturn(1000L);
    Mockito.when(config.get(TREE_LOCK_CLEAN_INTERVAL)).thenReturn(60L);
    return config;
  }

  private CompletionService<Integer> createCompletionService() {
    ThreadPoolExecutor executor =
        new ThreadPoolExecutor(
            10,
            10,
            0L,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(),
            new ThreadFactoryBuilder().setDaemon(true).build());

    CompletionService<Integer> completionService = new ExecutorCompletionService(executor);
    return completionService;
  }

  static NameIdentifier randomNameIdentifier() {
    Random random = new Random();
    int level = random.nextInt(10);
    NameIdentifier nameIdentifier;
    switch (level) {
      case 0:
        nameIdentifier = LockManager.ROOT;
        break;
      case 1:
        nameIdentifier = NameIdentifier.of(getName(random.nextInt(5)));
        break;
      case 2:
        nameIdentifier = NameIdentifier.of(getName(random.nextInt(5)), getName(random.nextInt(20)));
        break;
      case 3:
        nameIdentifier =
            NameIdentifier.of(
                getName(random.nextInt(5)),
                getName(random.nextInt(20)),
                getName(random.nextInt(30)));
        break;
      default:
        nameIdentifier =
            NameIdentifier.of(
                getName(random.nextInt(5)),
                getName(random.nextInt(20)),
                getName(random.nextInt(30)),
                getName(random.nextInt(100)));
    }

    return nameIdentifier;
  }

  @Test
  void multipleThreadTestLockManager() throws InterruptedException, ExecutionException {
    LockManager lockManager = new LockManager(getConfig());
    CompletionService<Integer> completionService = createCompletionService();
    for (int i = 0; i < 10; i++) {
      completionService.submit(() -> this.testLockManager(lockManager));
    }

    for (int i = 0; i < 10; i++) {
      completionService.take().get();
    }
  }

  int testNormalLock() throws InterruptedException {
    ThreadLocalRandom threadLocalRandom = ThreadLocalRandom.current();
    ReentrantReadWriteLock reentrantReadWriteLock = new ReentrantReadWriteLock();
    for (int i = 0; i < 1000; i++) {
      NameIdentifier identifier = randomNameIdentifier();
      int num = threadLocalRandom.nextInt(5);
      LockType lockType = num >= 4 ? LockType.WRITE : LockType.READ;
      //      LockType lockType = LockType.values()[threadLocalRandom.nextInt(2)];
      if (lockType == LockType.WRITE) {
        reentrantReadWriteLock.writeLock().lock();
        // App logic here...
        Thread.sleep(1);
        reentrantReadWriteLock.writeLock().unlock();
      } else {
        reentrantReadWriteLock.readLock().lock();
        // App logic here...
        Thread.sleep(1);
        reentrantReadWriteLock.readLock().unlock();
      }
    }

    return 0;
  }

  int testLockManager(LockManager lockManager) throws InterruptedException {
    ThreadLocalRandom threadLocalRandom = ThreadLocalRandom.current();
    for (int i = 0; i < 1000; i++) {
      NameIdentifier identifier = randomNameIdentifier();
      int num = threadLocalRandom.nextInt(5);
      LockType lockType = num >= 4 ? LockType.WRITE : LockType.READ;
      TreeLock lock = lockManager.createTreeLock(identifier);
      try {
        lock.lock(lockType);
        // App logic here...
        Thread.sleep(1);
      } catch (Exception e) {
        if (e.getMessage().contains("mock")) {
          return 0;
        }
        throw e;
      } finally {
        lock.unlock();
      }
    }

    return 0;
  }

  @Test
  @Disabled
  void testLockWithError() {
    LockManager lockManager = new LockManager(getConfig());
    LockManager spy = Mockito.spy(lockManager);

    // one fifth (2 /10 = 0.2) of tests will fail
    Mockito.doThrow(new RuntimeException("mock"))
        .when(spy)
        .createTreeLock(Mockito.eq(NameIdentifier.of(Namespace.of(), ENTITY_NAMES[0])));
    Mockito.doThrow(new RuntimeException("mock"))
        .when(spy)
        .createTreeLock(Mockito.eq(NameIdentifier.of(Namespace.of(), ENTITY_NAMES[1])));

    CompletionService<Integer> completionService = createCompletionService();
    for (int i = 0; i < 10; i++) {
      completionService.submit(() -> this.testLockManager(spy));
    }

    for (int i = 0; i < 10; i++) {
      Assertions.assertDoesNotThrow(() -> completionService.take().get());
    }
  }

  @Disabled
  @ParameterizedTest
  @ValueSource(ints = {1, 2, 4, 8, 10})
  void compare(int threadCount) throws InterruptedException, ExecutionException {
    LockManager lockManager = new LockManager(getConfig());
    CompletionService<Integer> completionService = createCompletionService();
    for (int i = 0; i < 2; i++) {
      completionService.submit(() -> this.testLockManager(lockManager));
    }
    for (int i = 0; i < 2; i++) {
      completionService.take().get();
    }

    long start = System.currentTimeMillis();
    for (int i = 0; i < threadCount; i++) {
      completionService.submit(() -> this.testLockManager(lockManager));
    }
    for (int i = 0; i < threadCount; i++) {
      completionService.take().get();
    }
    System.out.println("LockManager use tree lock: " + (System.currentTimeMillis() - start) + "ms");

    start = System.currentTimeMillis();
    for (int i = 0; i < threadCount; i++) {
      completionService.submit(() -> this.testNormalLock());
    }

    for (int i = 0; i < threadCount; i++) {
      completionService.take().get();
    }
    System.out.println(
        "LockManager use normal lock: " + (System.currentTimeMillis() - start) + "ms");
  }

  @Test
  void testLockCleaner() throws InterruptedException, ExecutionException {
    LockManager lockManager = new LockManager(getConfig());
    Random random = new Random();
    CompletionService<Integer> service = createCompletionService();

    for (int i = 0; i < 1000; i++) {
      NameIdentifier nameIdentifier = randomNameIdentifier();
      TreeLock lock = lockManager.createTreeLock(nameIdentifier);
      try {
        lock.lock(LockType.READ);
      } finally {
        lock.unlock();
      }
    }

    lockManager
        .treeLockRootNode
        .getAllChildren()
        .forEach(
            child -> {
              lockManager.evictStaleNodes(child, lockManager.treeLockRootNode);
            });

    Assertions.assertFalse(lockManager.treeLockRootNode.getAllChildren().isEmpty());

    for (int i = 0; i < 10; i++) {
      service.submit(
          () -> {
            for (int j = 0; j < 10000; j++) {
              NameIdentifier nameIdentifier = randomNameIdentifier();
              TreeLock lock = lockManager.createTreeLock(nameIdentifier);
              try {
                lock.lock(random.nextInt(2) == 0 ? LockType.READ : LockType.WRITE);
              } finally {
                lock.unlock();
              }
            }
            return 0;
          });
    }

    for (int i = 0; i < 10; i++) {
      service.take().get();
    }

    // Check the lock reference
    checkReferenceCount(lockManager.treeLockRootNode);

    List<Future<Integer>> futures = Lists.newArrayList();
    for (int i = 0; i < 5; i++) {
      futures.add(
          service.submit(
              () -> {
                for (int j = 0; j < 10000; j++) {
                  NameIdentifier nameIdentifier = randomNameIdentifier();
                  TreeLock lock = lockManager.createTreeLock(nameIdentifier);
                  try {
                    lock.lock(LockType.READ);
                  } finally {
                    lock.unlock();
                  }
                }
                return 0;
              }));
    }

    for (int i = 0; i < 5; i++) {
      service.submit(
          () -> {
            lockManager
                .treeLockRootNode
                .getAllChildren()
                .forEach(
                    child -> {
                      while (!futures.stream().allMatch(Future::isDone)) {
                        try {
                          lockManager.evictStaleNodes(child, lockManager.treeLockRootNode);
                          Thread.sleep(1);
                        } catch (Exception e) {
                          // Ignore
                        }
                      }
                    });
            return 0;
          });
    }

    for (int i = 0; i < 10; i++) {
      service.take().get();
    }

    checkReferenceCount(lockManager.treeLockRootNode);
  }

  private void checkReferenceCount(TreeLockNode node) {
    Assertions.assertEquals(0, node.getReference());
    node.getAllChildren().forEach(this::checkReferenceCount);
  }

  @Test
  void testNodeCountAndCleaner() throws ExecutionException, InterruptedException {
    LockManager lockManager = new LockManager(getConfig());
    CompletionService<Integer> service = createCompletionService();

    Future<Integer> future =
        service.submit(
            () -> {
              for (int i = 0; i < 20000; i++) {
                TreeLock treeLock = lockManager.createTreeLock(randomNameIdentifier());
                treeLock.lock(i % 2 == 0 ? LockType.WRITE : LockType.READ);
                treeLock.unlock();
              }

              return 0;
            });

    future.get();
    long totalCount = lockManager.totalNodeCount.get();
    Assertions.assertTrue(totalCount > 0);
  }

  @Test
  void testConcurrentRead() throws InterruptedException, ExecutionException {
    LockManager lockManager = new LockManager(getConfig());
    Object objectLock = new Object();

    CompletionService<Integer> service = createCompletionService();
    NameIdentifier nameIdentifier = NameIdentifier.of("a", "b", "c", "d");
    TreeLock treeLock = lockManager.createTreeLock(nameIdentifier);
    treeLock.lock(LockType.READ);
    treeLock.unlock();

    CountDownLatch countDownLatch = new CountDownLatch(1);
    service.submit(
        () -> {
          synchronized (objectLock) {
            TreeLock treeLock1 = lockManager.createTreeLock(nameIdentifier);
            treeLock1.lock(LockType.READ);
            // Hold lock and sleep
            countDownLatch.countDown();
            objectLock.wait();
            treeLock1.unlock();
            return 0;
          }
        });

    countDownLatch.await();

    for (int i = 0; i < 10; i++) {
      service.submit(
          () -> {
            TreeLock lock = lockManager.createTreeLock(nameIdentifier);
            lock.lock(LockType.READ);
            try {
              // User logic here...
            } finally {
              lock.unlock();
            }
            return 0;
          });
    }

    for (int i = 0; i < 10; i++) {
      service.take().get();
    }

    synchronized (objectLock) {
      objectLock.notify();
    }
  }

  @Test
  void testConcurrentWrite() throws InterruptedException {
    LockManager lockManager = new LockManager(getConfig());
    Map<String, Integer> stringMap = Maps.newHashMap();
    stringMap.put("total", 0);

    CompletionService<Integer> service = createCompletionService();
    NameIdentifier nameIdentifier = NameIdentifier.of("a", "b", "c", "d");

    for (int t = 0; t < 200; t++) {
      for (int i = 0; i < 10; i++) {
        service.submit(
            () -> {
              TreeLock treeLock = lockManager.createTreeLock(nameIdentifier);
              treeLock.lock(LockType.WRITE);
              try {
                stringMap.compute("total", (k, v) -> ++v);
              } catch (Exception e) {
                throw new RuntimeException(e);
              } finally {
                treeLock.unlock();
              }
              return 0;
            });
      }

      for (int i = 0; i < 10; i++) {
        service.take();
      }

      int total = stringMap.get("total");
      Assertions.assertEquals(10, total, "Total should always 10");

      stringMap.put("total", 0);
    }
  }

  private NameIdentifier completeRandomNameIdentifier() {
    Random random = new Random();

    int level = 4;
    int nameLength = 16;
    String[] names = new String[level];
    for (int i = 0; i < level; i++) {
      String name = "";
      for (int j = 0; j < nameLength; j++) {
        int v;
        while ((v = random.nextInt(123)) < 97) {}
        name += ((char) v);
      }

      names[i] = name;
    }

    return NameIdentifier.of(names);
  }

  @Test
  void testNodeCount() throws InterruptedException, ExecutionException {
    LockManager lockManager = new LockManager(getConfig());
    CompletionService<Integer> service = createCompletionService();

    for (int i = 0; i < 10; i++) {
      service.submit(
          () -> {
            TreeLock treeLock;
            for (int j = 0; j < 1000; j++) {
              NameIdentifier nameIdentifier = completeRandomNameIdentifier();
              treeLock = lockManager.createTreeLock(nameIdentifier);
              treeLock.lock(LockType.READ);
              treeLock.unlock();
            }
            return 0;
          });
    }

    for (int i = 0; i < 10; i++) {
      service.take().get();
    }

    Assertions.assertEquals(10 * 1000 * 4 + 1, lockManager.totalNodeCount.get());

    // Pay attention to the lockCleaner thread.
    lockManager
        .treeLockRootNode
        .getAllChildren()
        .forEach(node -> lockManager.evictStaleNodes(node, lockManager.treeLockRootNode));
    Assertions.assertTrue(lockManager.totalNodeCount.get() > 1);
    Assertions.assertTrue(lockManager.totalNodeCount.get() < lockManager.maxTreeNodeInMemory);
  }

  private TreeLockNode getTreeNode(TreeLockNode root, int depth) {
    if (depth == 0) {
      return root;
    }

    int i = 0;
    TreeLockNode result = root;
    while (i++ < depth) {
      // We know it only has one child;
      result = result.getAllChildren().get(0);
    }

    return result;
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 2, 3})
  void testCreateTreeLock(int level) throws Exception {
    LockManager lockManager = new LockManager(getConfig());
    TreeLockNode rootNode = lockManager.treeLockRootNode;
    TreeLock treeLock = lockManager.createTreeLock(NameIdentifier.of("a", "b", "c", "d"));
    treeLock.lock(LockType.READ);
    treeLock.unlock();
    checkReferenceCount(rootNode);

    // Start to use concurrent threads to create tree lock.
    TreeLockNode treeLockNode = getTreeNode(rootNode, level);
    TreeLockNode spyNode = Mockito.spy(treeLockNode);
    Mockito.doThrow(new RuntimeException("Mock exception"))
        .when(spyNode)
        .getOrCreateChild(Mockito.any());

    if (level == 0) {
      lockManager.treeLockRootNode = spyNode;
    } else {
      int parentLevel = level - 1;
      TreeLockNode parentNode = getTreeNode(rootNode, parentLevel);
      parentNode.childMap.put(treeLockNode.getName(), spyNode);
    }

    CompletionService<Integer> service = createCompletionService();
    int concurrentThreadCount = 1;
    NameIdentifier abcd = NameIdentifier.of("a", "b", "c", "d");
    for (int i = 0; i < concurrentThreadCount; i++) {
      service.submit(
          () -> {
            for (int j = 0; j < 1000; j++) {
              Assertions.assertThrows(
                  RuntimeException.class, () -> lockManager.createTreeLock(abcd));
            }
            return 0;
          });
    }

    for (int i = 0; i < concurrentThreadCount; i++) {
      service.take().get();
    }

    TreeLockNode lockNode = lockManager.treeLockRootNode;
    checkReferenceCount(lockNode);
  }

  @Test
  void testConfigs() {
    Config config = getConfig();
    Mockito.when(config.get(TREE_LOCK_MAX_NODE_IN_MEMORY)).thenReturn(20000L);
    Mockito.when(config.get(TREE_LOCK_MIN_NODE_IN_MEMORY)).thenReturn(2000L);
    Mockito.when(config.get(TREE_LOCK_CLEAN_INTERVAL)).thenReturn(2000L);

    LockManager manager = new LockManager(config);
    Assertions.assertEquals(20000L, manager.maxTreeNodeInMemory);
    Assertions.assertEquals(2000L, manager.minTreeNodeInMemory);
    Assertions.assertEquals(2000L, manager.cleanTreeNodeIntervalInSecs);
  }

  @Test
  void testMaxTreeNode() {
    Config config = getConfig();
    Mockito.when(config.get(TREE_LOCK_MAX_NODE_IN_MEMORY)).thenReturn(2000L);
    Mockito.when(config.get(TREE_LOCK_MIN_NODE_IN_MEMORY)).thenReturn(100L);

    LockManager manager = new LockManager(config);

    Assertions.assertThrows(
        IllegalStateException.class,
        () -> {
          for (int i = 0; i < 1000; i++) {
            TreeLock lock = manager.createTreeLock(completeRandomNameIdentifier());
          }
        });
  }

  @Test
  void testReferenceCount() throws InterruptedException, ExecutionException {
    LockManager lockManager = new LockManager(getConfig());
    CompletionService<Integer> service = createCompletionService();
    int concurrentThreadCount = 10;
    for (int i = 0; i < concurrentThreadCount; i++) {
      service.submit(
          () -> {
            for (int j = 0; j < 1000; j++) {
              TreeLock lock = lockManager.createTreeLock(NameIdentifier.of("a", "b", "c", "d"));
              lock.lock(j % 2 == 0 ? LockType.READ : LockType.WRITE);
              try {
                // Deliberately throw an exception here.
                @SuppressWarnings("divzero")
                int a = 1 / 0;
              } catch (Exception e) {
                // Ignore
              } finally {
                lock.unlock();
              }
            }
            return 0;
          });
    }

    for (int i = 0; i < concurrentThreadCount; i++) {
      service.take().get();
    }

    checkReferenceCount(lockManager.treeLockRootNode);
  }

  @Test
  void testDeadLockChecker() throws InterruptedException, ExecutionException {
    LockManager lockManager = new LockManager(getConfig());
    CompletionService<Integer> service = createCompletionService();
    int concurrentThreadCount = 9;
    for (int i = 0; i < concurrentThreadCount; i++) {
      service.submit(
          () -> {
            for (int j = 0; j < 1000; j++) {
              TreeLock lock = lockManager.createTreeLock(completeRandomNameIdentifier());
              lock.lock(j % 2 == 0 ? LockType.READ : LockType.WRITE);
              try {
                Thread.sleep(1);
              } catch (Exception e) {
                // Ignore
              } finally {
                lock.unlock();
              }
            }
            return 0;
          });
    }

    service.submit(
        () -> {
          for (int i = 0; i < 1000; i++) {
            lockManager.checkDeadLock(lockManager.treeLockRootNode);
          }
          return 0;
        });

    for (int i = 0; i < concurrentThreadCount; i++) {
      service.take().get();
    }
  }
}
