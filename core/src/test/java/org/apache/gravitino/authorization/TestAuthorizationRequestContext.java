/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *  http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.authorization;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableSet;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

public class TestAuthorizationRequestContext {

  @Test
  public void testLoadRoleRunsOnceEvenWhenInvokedConcurrently() throws Exception {
    AuthorizationRequestContext context = new AuthorizationRequestContext();
    AtomicInteger counter = new AtomicInteger();
    CountDownLatch firstStarted = new CountDownLatch(1);
    CountDownLatch allowFinish = new CountDownLatch(1);

    Thread firstInvocation =
        new Thread(
            () ->
                context.loadRole(
                    () -> {
                      counter.incrementAndGet();
                      firstStarted.countDown();
                      try {
                        allowFinish.await(5, TimeUnit.SECONDS);
                      } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                      }
                    }));
    firstInvocation.start();

    try {
      assertTrue(firstStarted.await(5, TimeUnit.SECONDS));
      context.loadRole(counter::incrementAndGet);
      assertEquals(1, counter.get());
    } finally {
      allowFinish.countDown();
      firstInvocation.join();
    }

    context.loadRole(counter::incrementAndGet);
    assertEquals(1, counter.get(), "Subsequent loadRole calls should be ignored");
  }

  @Test
  public void testLoadRoleFailThenSuccessThenIgnored() throws Exception {
    AuthorizationRequestContext context = new AuthorizationRequestContext();
    AtomicInteger counter = new AtomicInteger();

    CountDownLatch failingStarted = new CountDownLatch(1);
    CountDownLatch allowFailToThrow = new CountDownLatch(1);

    Thread failingThread =
        new Thread(
            () -> {
              try {
                context.loadRole(
                    () -> {
                      counter.incrementAndGet();
                      failingStarted.countDown();
                      try {
                        allowFailToThrow.await(2, TimeUnit.SECONDS);
                      } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                      }
                      throw new IllegalStateException("Simulated failure");
                    });
              } catch (RuntimeException e) {
                assertTrue(e.getMessage().contains("Failed to load role"));
                assertInstanceOf(IllegalStateException.class, e.getCause());
              }
            });

    failingThread.start();
    assertTrue(failingStarted.await(2, TimeUnit.SECONDS));
    allowFailToThrow.countDown();
    failingThread.join();
    context.loadRole(counter::incrementAndGet);
    assertEquals(2, counter.get(), "Flag should remain false after failure so next call runs.");
    context.loadRole(counter::incrementAndGet);
    assertEquals(2, counter.get(), "After a successful loadRole, further calls must be ignored.");
  }

  @Test
  public void testUserRoleIdsDefaultsToEmptySet() {
    AuthorizationRequestContext context = new AuthorizationRequestContext();
    assertEquals(Collections.emptySet(), context.getUserRoleIds());
    assertTrue(context.getUserRoleIds().isEmpty());
  }

  @Test
  public void testSetUserRoleIdsWithNullNormalizesToEmptySet() {
    AuthorizationRequestContext context = new AuthorizationRequestContext();
    context.setUserRoleIds(ImmutableSet.of(1L, 2L));
    context.setUserRoleIds(null);
    // Null must not propagate; downstream callers iterate the set without a null check.
    assertSame(Collections.emptySet(), context.getUserRoleIds());
  }

  @Test
  public void testSetUserRoleIdsRoundTrip() {
    AuthorizationRequestContext context = new AuthorizationRequestContext();
    Set<Long> roles = ImmutableSet.of(7L, 11L, 13L);
    context.setUserRoleIds(roles);
    assertEquals(roles, context.getUserRoleIds());
  }

  @Test
  public void testSetUserRoleIdsDefensivelyCopiesAndReturnsImmutableSet() {
    AuthorizationRequestContext context = new AuthorizationRequestContext();
    Set<Long> roles = new HashSet<>(ImmutableSet.of(17L, 19L));
    context.setUserRoleIds(roles);
    roles.add(23L);
    assertEquals(ImmutableSet.of(17L, 19L), context.getUserRoleIds());
    assertThrows(UnsupportedOperationException.class, () -> context.getUserRoleIds().add(29L));
  }
}
