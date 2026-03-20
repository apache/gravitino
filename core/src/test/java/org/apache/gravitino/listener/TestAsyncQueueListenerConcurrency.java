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

package org.apache.gravitino.listener;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.listener.api.EventListenerPlugin;
import org.apache.gravitino.listener.api.event.Event;
import org.apache.gravitino.listener.api.event.OperationStatus;
import org.apache.gravitino.listener.api.event.PreEvent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestAsyncQueueListenerConcurrency {

  static class DummyEvent extends Event {

    protected DummyEvent(String user, NameIdentifier identifier) {
      super(user, identifier);
    }

    @Override
    public OperationStatus operationStatus() {
      return OperationStatus.SUCCESS;
    }
  }

  static class NoOpListener implements EventListenerPlugin {
    @Override
    public void init(Map<String, String> properties) {}

    @Override
    public void start() {}

    @Override
    public void stop() {}

    @Override
    public void onPostEvent(Event event) {}

    @Override
    public void onPreEvent(PreEvent event) {}
  }

  @Test
  void testConcurrentDropEventsDoNotThrow() throws InterruptedException {
    // Create a queue with capacity 1 so events are dropped quickly.
    List<EventListenerPlugin> listeners = Collections.singletonList(new NoOpListener());
    AsyncQueueListener asyncQueueListener =
        new AsyncQueueListener(listeners, "concurrency-test", 1, 5);
    asyncQueueListener.start();

    int threadCount = 10;
    int eventsPerThread = 100;
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    CountDownLatch latch = new CountDownLatch(threadCount);
    List<Throwable> errors = Collections.synchronizedList(new ArrayList<>());

    for (int t = 0; t < threadCount; t++) {
      executor.submit(
          () -> {
            try {
              for (int i = 0; i < eventsPerThread; i++) {
                asyncQueueListener.onPostEvent(
                    new DummyEvent("user", NameIdentifier.of("ns", "name")));
              }
            } catch (Exception e) {
              errors.add(e);
            } finally {
              latch.countDown();
            }
          });
    }

    latch.await(30, TimeUnit.SECONDS);
    executor.shutdown();
    asyncQueueListener.stop();

    Assertions.assertTrue(
        errors.isEmpty(), "Concurrent event enqueue should not throw: " + errors);
  }
}
