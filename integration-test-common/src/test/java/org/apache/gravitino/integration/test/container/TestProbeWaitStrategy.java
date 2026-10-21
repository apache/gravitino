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
package org.apache.gravitino.integration.test.container;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.testcontainers.containers.ContainerLaunchException;
import org.testcontainers.containers.wait.strategy.WaitStrategyTarget;

/** Tests the readiness probes against stubs; nothing here starts a container. */
public class TestProbeWaitStrategy {

  private final WaitStrategyTarget target = Mockito.mock(WaitStrategyTarget.class);

  @Test
  void testProbesAtOnceAndUntilReady() {
    AtomicInteger calls = new AtomicInteger();
    List<WaitStrategyTarget> seen = new ArrayList<>();
    ProbeWaitStrategy strategy =
        new ProbeWaitStrategy(
                "the third call",
                t -> {
                  seen.add(t);
                  return calls.incrementAndGet() == 3;
                })
            .withPollInterval(Duration.ofMillis(10));
    strategy.withStartupTimeout(Duration.ofSeconds(5));

    long startNanos = System.nanoTime();
    strategy.waitUntilReady(target);

    Assertions.assertEquals(3, calls.get());
    Assertions.assertTrue(seen.stream().allMatch(t -> t == target));
    Assertions.assertTrue(Duration.ofNanos(System.nanoTime() - startNanos).toMillis() < 2_000);
  }

  @Test
  void testExceptionsCountAsNotReady() {
    AtomicInteger calls = new AtomicInteger();
    ProbeWaitStrategy strategy =
        new ProbeWaitStrategy(
                "a service that refuses once",
                t -> {
                  if (calls.incrementAndGet() == 1) {
                    throw new IOException("connection refused");
                  }
                  return true;
                })
            .withPollInterval(Duration.ofMillis(10));
    strategy.withStartupTimeout(Duration.ofSeconds(5));

    strategy.waitUntilReady(target);

    Assertions.assertEquals(2, calls.get());
  }

  @Test
  void testTimeoutNamesTheConditionKeepsTheLastFailureAndReports() {
    List<WaitStrategyTarget> reported = new ArrayList<>();
    ProbeWaitStrategy strategy =
        new ProbeWaitStrategy(
                "a service that never starts",
                t -> {
                  throw new IOException("connection refused");
                })
            .withPollInterval(Duration.ofMillis(10))
            .withTimeoutReport(reported::add);
    strategy.withStartupTimeout(Duration.ofMillis(200));

    ContainerLaunchException e =
        Assertions.assertThrows(
            ContainerLaunchException.class, () -> strategy.waitUntilReady(target));

    Assertions.assertTrue(e.getMessage().contains("a service that never starts"), e.getMessage());
    Assertions.assertInstanceOf(IOException.class, e.getCause());
    Assertions.assertEquals(List.of(target), reported);
  }

  @Test
  void testStarRocksAndDorisCountOnlyLiveBackendsWithCapacity() throws SQLException {
    ResultSet backends = backends("true", "1.000 TB", "false", "1.000 TB", "true", "0.000 B");

    Assertions.assertEquals(1, StarRocksContainer.readyBackends(backends));
    Assertions.assertEquals(0, StarRocksContainer.readyBackends(backends()));
  }

  @Test
  void testHiveNeedsALiveDataNode() {
    Assertions.assertTrue(
        HiveContainer.hasLiveDataNode("{\"beans\":[{\"NumLiveDataNodes\" : 1}]}"));
    Assertions.assertFalse(HiveContainer.hasLiveDataNode("{\"beans\":[{\"NumLiveDataNodes\":0}]}"));
    Assertions.assertFalse(HiveContainer.hasLiveDataNode(null));
  }

  // A result of (Alive, TotalCapacity) pairs.
  private static ResultSet backends(String... aliveAndCapacity) throws SQLException {
    ResultSet resultSet = Mockito.mock(ResultSet.class);
    AtomicInteger row = new AtomicInteger(-1);
    Mockito.when(resultSet.next())
        .thenAnswer(invocation -> row.incrementAndGet() < aliveAndCapacity.length / 2);
    Mockito.when(resultSet.getString("Alive"))
        .thenAnswer(invocation -> aliveAndCapacity[row.get() * 2]);
    Mockito.when(resultSet.getString("TotalCapacity"))
        .thenAnswer(invocation -> aliveAndCapacity[row.get() * 2 + 1]);
    return resultSet;
  }
}
