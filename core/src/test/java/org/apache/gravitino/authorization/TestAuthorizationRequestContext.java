package org.apache.gravitino.authorization;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

public class TestAuthorizationRequestContext {

  @Test
  public void testLoadRoleRunsOnlyOnce() throws InterruptedException {
    AuthorizationRequestContext context = new AuthorizationRequestContext();
    AtomicInteger counter = new AtomicInteger(0);

    Runnable init = counter::incrementAndGet;

    int threadCount = 10;
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);

    CountDownLatch ready = new CountDownLatch(threadCount);
    CountDownLatch start = new CountDownLatch(1);

    for (int i = 0; i < threadCount; i++) {
      executor.submit(
          () -> {
            try {
              ready.countDown();
              start.await();
              context.loadRole(init);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          });
    }

    ready.await();
    start.countDown();

    executor.shutdown();
    executor.awaitTermination(5, TimeUnit.SECONDS);

    assertEquals(
        1, counter.get(), "Initializer should run exactly once, even with concurrent calls");
  }
}
