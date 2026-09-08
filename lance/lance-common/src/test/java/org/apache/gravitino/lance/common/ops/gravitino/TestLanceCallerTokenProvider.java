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
 *
 */
package org.apache.gravitino.lance.common.ops.gravitino;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.auth.ActiveRoles;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.exceptions.UnauthorizedException;
import org.apache.gravitino.utils.PrincipalUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestLanceCallerTokenProvider {

  private final LanceCallerTokenProvider provider = new LanceCallerTokenProvider();

  @Test
  void testForwardCredentialsAndAllRoleModes() throws Exception {
    for (String token : List.of("Basic YWxpY2U6ZHVtbXk=", "Bearer alice-token")) {
      for (ActiveRoles roles :
          List.of(
              ActiveRoles.all(), ActiveRoles.none(), ActiveRoles.of(List.of("reader", "writer")))) {
        PrincipalUtils.doAs(
            new UserPrincipal("alice", token).withActiveRoles(roles),
            () -> {
              Assertions.assertEquals(
                  token, new String(provider.getTokenData(), StandardCharsets.UTF_8));
              String expected = roles.isAll() ? "ALL" : roles.isNone() ? "NONE" : "reader,writer";
              Assertions.assertEquals(
                  expected,
                  provider.getRequestHeaders().get(AuthConstants.X_GRAVITINO_ACTIVE_ROLES_HEADER));
              return null;
            });
      }
    }
  }

  @Test
  void testRejectMissingAndNonForwardableCredentials() throws Exception {
    Assertions.assertThrows(UnauthorizedException.class, provider::getTokenData);
    for (UserPrincipal principal :
        List.of(
            new UserPrincipal("alice"),
            new UserPrincipal("alice", "Bearer "),
            new UserPrincipal("alice", "Negotiate kerberos-ticket"),
            new UserPrincipal(AuthConstants.ANONYMOUS_USER, "Basic YW5vbnltb3Vz"))) {
      PrincipalUtils.doAs(
          principal,
          () -> {
            Assertions.assertThrows(UnauthorizedException.class, provider::getTokenData);
            return null;
          });
    }
  }

  @Test
  void testSharedProviderDoesNotMixConcurrentCallersOrRetainIdentity() throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(2);
    CyclicBarrier barrier = new CyclicBarrier(2);
    try {
      Future<?> alice = executor.submit(() -> checkCaller("alice", barrier));
      Future<?> bob = executor.submit(() -> checkCaller("bob", barrier));
      alice.get(30, TimeUnit.SECONDS);
      bob.get(30, TimeUnit.SECONDS);
      Assertions.assertThrows(UnauthorizedException.class, provider::getTokenData);
    } finally {
      executor.shutdownNow();
    }
  }

  private void checkCaller(String name, CyclicBarrier barrier) {
    try {
      PrincipalUtils.doAs(
          new UserPrincipal(name, "Bearer " + name).withActiveRoles(ActiveRoles.of(List.of(name))),
          () -> {
            barrier.await(10, TimeUnit.SECONDS);
            for (int i = 0; i < 20; i++) {
              Assertions.assertEquals(
                  "Bearer " + name, new String(provider.getTokenData(), StandardCharsets.UTF_8));
              Assertions.assertEquals(
                  name,
                  provider.getRequestHeaders().get(AuthConstants.X_GRAVITINO_ACTIVE_ROLES_HEADER));
            }
            return null;
          });
      Assertions.assertThrows(UnauthorizedException.class, provider::getTokenData);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
