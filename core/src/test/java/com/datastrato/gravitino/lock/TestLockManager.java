/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.lock;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.junit.jupiter.api.Assertions;
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

  private static final LockManager lockManager = new LockManager();

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

  private NameIdentifier randomNameIdentifier() {
    Random random = new Random();
    int level = random.nextInt(10);
    NameIdentifier nameIdentifier = null;
    switch (level) {
      case 0:
        nameIdentifier = NameIdentifier.ROOT;
        break;
      case 1:
        nameIdentifier = NameIdentifier.of(Namespace.of(), ENTITY_NAMES[random.nextInt(1)]);
        break;
      case 2:
        nameIdentifier =
            NameIdentifier.of(
                Namespace.of(ENTITY_NAMES[random.nextInt(1)]), ENTITY_NAMES[random.nextInt(3)]);
        break;
      case 3:
        nameIdentifier =
            NameIdentifier.of(
                Namespace.of(ENTITY_NAMES[random.nextInt(1)], ENTITY_NAMES[random.nextInt(3)]),
                ENTITY_NAMES[random.nextInt(5)]);
        break;
      default:
        nameIdentifier =
            NameIdentifier.of(
                Namespace.of(
                    ENTITY_NAMES[random.nextInt(1)],
                    ENTITY_NAMES[random.nextInt(3)],
                    ENTITY_NAMES[random.nextInt(5)]),
                ENTITY_NAMES[random.nextInt(10)]);
    }

    return nameIdentifier;
  }

  @Test
  void multipleThreadTestLockManager() throws InterruptedException, ExecutionException {
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
  void testLockWithError() {
    LockManager lockManager = new LockManager();
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

  //  @Disabled
  @ParameterizedTest
  @ValueSource(ints = {1, 2, 4, 8, 10})
  void compare(int threadCount) throws InterruptedException, ExecutionException {
    LockManager lockManager = new LockManager();
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
    LockManager lockManager = new LockManager();
    Random random = new Random();
    CompletionService<Integer> service = createCompletionService();

    //    for (int i = 0; i < 1000; i++) {
    //      NameIdentifier nameIdentifier = randomNameIdentifier();
    //      TreeLock lock = lockManager.createTreeLock(nameIdentifier);
    //      try {
    //        lock.lock(LockType.READ);
    //      } finally {
    //        lock.unlock();
    //      }
    //    }
    //
    //    lockManager
    //        .treeLockRootNode
    //        .getAllChildren()
    //        .forEach(
    //            child -> {
    //              lockManager.evictStaleNodes(child, lockManager.treeLockRootNode);
    //            });
    //
    //    Assertions.assertTrue(lockManager.treeLockRootNode.getAllChildren().isEmpty());
    //
    //    for (int i = 0; i < 10; i++) {
    //      service.submit(
    //          () -> {
    //            for (int j = 0; j < 10000; j++) {
    //              NameIdentifier nameIdentifier = randomNameIdentifier();
    //              TreeLock lock = lockManager.createTreeLock(nameIdentifier);
    //              try {
    //                lock.lock(random.nextInt(2) == 0 ? LockType.READ : LockType.WRITE);
    //              } finally {
    //                lock.unlock();
    //              }
    //            }
    //            return 0;
    //          });
    //    }
    //
    //    for (int i = 0; i < 10; i++) {
    //      service.take().get();
    //    }

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
    Assertions.assertEquals(0, node.getReferenceCount());
    node.getAllChildren().forEach(this::checkReferenceCount);
  }
}
