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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.auth.ActiveRoles;
import org.apache.gravitino.storage.relational.po.auth.GroupUpdatedAt;
import org.apache.gravitino.storage.relational.po.auth.OwnerInfo;
import org.apache.gravitino.storage.relational.po.auth.UserUpdatedAt;
import org.apache.gravitino.utils.PrincipalUtils;
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
  public void testComputeUserInfoIfAbsentDedupesLoaderInvocation() {
    AuthorizationRequestContext context = new AuthorizationRequestContext();
    AtomicInteger loaderCalls = new AtomicInteger();
    UserUpdatedAt expected = new UserUpdatedAt(42L, 1234L);

    Optional<UserUpdatedAt> first =
        context.computeUserInfoIfAbsent(
            "ml::alice",
            k -> {
              loaderCalls.incrementAndGet();
              return Optional.of(expected);
            });
    Optional<UserUpdatedAt> second =
        context.computeUserInfoIfAbsent(
            "ml::alice",
            k -> {
              loaderCalls.incrementAndGet();
              return Optional.empty();
            });

    assertTrue(first.isPresent());
    assertEquals(expected, first.get());
    assertTrue(second.isPresent());
    assertEquals(expected, second.get());
    assertEquals(1, loaderCalls.get(), "Loader must run only once for the same key");
  }

  @Test
  public void testComputeUserInfoIfAbsentCachesEmptyResult() {
    AuthorizationRequestContext context = new AuthorizationRequestContext();
    AtomicInteger loaderCalls = new AtomicInteger();

    Optional<UserUpdatedAt> first =
        context.computeUserInfoIfAbsent(
            "ml::ghost",
            k -> {
              loaderCalls.incrementAndGet();
              return Optional.empty();
            });
    Optional<UserUpdatedAt> second =
        context.computeUserInfoIfAbsent(
            "ml::ghost",
            k -> {
              loaderCalls.incrementAndGet();
              return Optional.of(new UserUpdatedAt(1L, 1L));
            });

    assertFalse(first.isPresent());
    assertFalse(second.isPresent(), "Empty optional should be cached and reused");
    assertEquals(1, loaderCalls.get());
  }

  @Test
  public void testComputeUserInfoIfAbsentDifferentKeysRunLoaderEachTime() {
    AuthorizationRequestContext context = new AuthorizationRequestContext();
    AtomicInteger loaderCalls = new AtomicInteger();

    context.computeUserInfoIfAbsent(
        "ml::alice",
        k -> {
          loaderCalls.incrementAndGet();
          return Optional.of(new UserUpdatedAt(1L, 1L));
        });
    context.computeUserInfoIfAbsent(
        "ml::bob",
        k -> {
          loaderCalls.incrementAndGet();
          return Optional.of(new UserUpdatedAt(2L, 2L));
        });

    assertEquals(2, loaderCalls.get());
  }

  @Test
  public void testComputeGroupInfoIfAbsentCachesPresentAndAbsentResults() {
    AuthorizationRequestContext context = new AuthorizationRequestContext();
    AtomicInteger loaderCalls = new AtomicInteger();

    GroupUpdatedAt groupInfo = new GroupUpdatedAt(42L, 1234L);
    Optional<GroupUpdatedAt> presentFirst =
        context.computeGroupInfoIfAbsent(
            "ml::group1",
            k -> {
              loaderCalls.incrementAndGet();
              return Optional.of(groupInfo);
            });
    Optional<GroupUpdatedAt> presentSecond =
        context.computeGroupInfoIfAbsent(
            "ml::group1",
            k -> {
              loaderCalls.incrementAndGet();
              return Optional.empty();
            });

    Optional<GroupUpdatedAt> absentFirst =
        context.computeGroupInfoIfAbsent(
            "ml::missing-group",
            k -> {
              loaderCalls.incrementAndGet();
              return Optional.empty();
            });
    Optional<GroupUpdatedAt> absentSecond =
        context.computeGroupInfoIfAbsent(
            "ml::missing-group",
            k -> {
              loaderCalls.incrementAndGet();
              return Optional.of(new GroupUpdatedAt(99L, 9999L));
            });

    assertEquals(Optional.of(groupInfo), presentFirst);
    assertEquals(Optional.of(groupInfo), presentSecond);
    assertFalse(absentFirst.isPresent());
    assertFalse(absentSecond.isPresent(), "Absent group result must also be cached");
    assertEquals(2, loaderCalls.get(), "Loader must fire once per distinct group key");
  }

  @Test
  public void testComputeMetadataIdIfAbsentDedupesLoaderInvocation() {
    AuthorizationRequestContext context = new AuthorizationRequestContext();
    AtomicInteger loaderCalls = new AtomicInteger();

    Long first =
        context.computeMetadataIdIfAbsent(
            "ml::cat::TABLE",
            k -> {
              loaderCalls.incrementAndGet();
              return 1001L;
            });
    Long second =
        context.computeMetadataIdIfAbsent(
            "ml::cat::TABLE",
            k -> {
              loaderCalls.incrementAndGet();
              return 9999L;
            });

    assertEquals(1001L, first);
    assertEquals(1001L, second);
    assertEquals(1, loaderCalls.get());
  }

  @Test
  public void testComputeOwnerIfAbsentCachesPresentAndAbsentResults() {
    AuthorizationRequestContext context = new AuthorizationRequestContext();
    AtomicInteger loaderCalls = new AtomicInteger();

    OwnerInfo ownerInfo = new OwnerInfo(99L, "USER");
    Optional<OwnerInfo> presentFirst =
        context.computeOwnerIfAbsent(
            10L,
            id -> {
              loaderCalls.incrementAndGet();
              return Optional.of(ownerInfo);
            });
    Optional<OwnerInfo> presentSecond =
        context.computeOwnerIfAbsent(
            10L,
            id -> {
              loaderCalls.incrementAndGet();
              return Optional.empty();
            });

    Optional<OwnerInfo> absentFirst =
        context.computeOwnerIfAbsent(
            20L,
            id -> {
              loaderCalls.incrementAndGet();
              return Optional.empty();
            });
    Optional<OwnerInfo> absentSecond =
        context.computeOwnerIfAbsent(
            20L,
            id -> {
              loaderCalls.incrementAndGet();
              return Optional.of(new OwnerInfo(123L, "USER"));
            });

    assertEquals(Optional.of(ownerInfo), presentFirst);
    assertEquals(Optional.of(ownerInfo), presentSecond);
    assertFalse(absentFirst.isPresent());
    assertFalse(absentSecond.isPresent(), "Absent owner result must also be cached");
    assertEquals(2, loaderCalls.get(), "Loader must fire once per distinct metadataId");
  }

  @Test
  public void testComputeHelpersRejectNullLoaderResults() {
    AuthorizationRequestContext context = new AuthorizationRequestContext();

    NullPointerException userError =
        assertThrows(
            NullPointerException.class,
            () -> context.computeUserInfoIfAbsent("ml::user", key -> null));
    assertTrue(userError.getMessage().contains("User info loader must not return null"));

    NullPointerException groupError =
        assertThrows(
            NullPointerException.class,
            () -> context.computeGroupInfoIfAbsent("ml::group", key -> null));
    assertTrue(groupError.getMessage().contains("Group info loader must not return null"));

    NullPointerException metadataError =
        assertThrows(
            NullPointerException.class,
            () -> context.computeMetadataIdIfAbsent("ml::catalog", key -> null));
    assertTrue(metadataError.getMessage().contains("Metadata id loader must not return null"));

    NullPointerException ownerError =
        assertThrows(
            NullPointerException.class, () -> context.computeOwnerIfAbsent(1L, id -> null));
    assertTrue(ownerError.getMessage().contains("Owner loader must not return null"));
  }

  @Test
  public void testOriginalAuthorizationExpressionRoundTrip() {
    AuthorizationRequestContext context = new AuthorizationRequestContext();
    context.setOriginalAuthorizationExpression("OWNER && HAS_PRIVILEGE");
    assertEquals("OWNER && HAS_PRIVILEGE", context.getOriginalAuthorizationExpression());
  }

  @Test
  public void testActiveRolesInitializedFromPrincipal() throws Exception {
    // With no active roles on the current principal, a new context defaults to ALL (no narrowing).
    assertEquals(ActiveRoles.all(), new AuthorizationRequestContext().getActiveRoles());

    // A new context picks up the active roles carried by the current UserPrincipal.
    ActiveRoles named = ActiveRoles.of(Arrays.asList("analyst"));
    UserPrincipal principal = new UserPrincipal("tester").withActiveRoles(named);
    ActiveRoles seen =
        PrincipalUtils.doAs(principal, () -> new AuthorizationRequestContext().getActiveRoles());
    assertEquals(named, seen);
  }
}
