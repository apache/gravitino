/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
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
