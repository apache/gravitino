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

package org.apache.gravitino.utils;

import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestRequestContext {

  @AfterEach
  public void cleanup() {
    RequestContext.clear();
  }

  @Test
  public void testSetAndGet() {
    RequestContext.setRemoteAddress("192.168.1.1");
    Assertions.assertEquals("192.168.1.1", RequestContext.getRemoteAddress());
  }

  @Test
  public void testClearRemovesValue() {
    RequestContext.setRemoteAddress("10.0.0.1");
    RequestContext.clear();
    Assertions.assertNull(RequestContext.getRemoteAddress());
  }

  @Test
  public void testGetReturnsNullWhenNotSet() {
    Assertions.assertNull(RequestContext.getRemoteAddress());
  }

  @Test
  public void testThreadIsolation() throws InterruptedException {
    RequestContext.setRemoteAddress("main-thread-ip");
    AtomicReference<String> childValue = new AtomicReference<>();

    Thread child =
        new Thread(
            () -> {
              // Child thread has its own ThreadLocal slot — must not see the main-thread value.
              childValue.set(RequestContext.getRemoteAddress());
            });
    child.start();
    child.join();

    Assertions.assertNull(childValue.get(), "Child thread should not inherit parent ThreadLocal");
    Assertions.assertEquals(
        "main-thread-ip", RequestContext.getRemoteAddress(), "Main thread value unchanged");
  }
<<<<<<< HEAD
=======

  /**
   * Callers read the stash unconditionally on every table operation, so the common case is that
   * nothing was stashed. Pins that this returns an empty map rather than {@code null}, which keeps
   * the dispatcher free of a null check on the hot path.
   */
  @Test
  public void testTakeAuditExtrasReturnsEmptyWhenUnset() {
    Assertions.assertTrue(RequestContext.takeAuditExtras().isEmpty());
  }

  /**
   * The stash is a hand-off between two dispatcher layers, not a request-scoped register: exactly
   * one reader is expected to consume each set. Pins the take-and-clear semantics that the
   * dispatcher relies on to avoid attributing a fact to a later operation.
   */
  @Test
  public void testSetAndTakeAuditExtras() {
    RequestContext.setAuditExtras(Collections.singletonMap("audit.reason", "policy-applied"));
    Assertions.assertEquals("policy-applied", RequestContext.takeAuditExtras().get("audit.reason"));
    Assertions.assertTrue(RequestContext.takeAuditExtras().isEmpty(), "take must clear extras");
  }

  /**
   * A contributor that decides it has nothing to report will naturally pass an empty or {@code
   * null} map. Pins that this retracts an earlier stash instead of being a no-op, so a stale fact
   * from an earlier decision cannot be attached to the event.
   */
  @Test
  public void testEmptyOrNullAuditExtrasClearsStash() {
    RequestContext.setAuditExtras(Collections.singletonMap("k", "v"));
    RequestContext.setAuditExtras(Collections.emptyMap());
    Assertions.assertTrue(RequestContext.takeAuditExtras().isEmpty());

    RequestContext.setAuditExtras(Collections.singletonMap("k", "v"));
    RequestContext.setAuditExtras(null);
    Assertions.assertTrue(RequestContext.takeAuditExtras().isEmpty());
  }

  /**
   * Extras are a third {@link ThreadLocal} on a pooled servlet thread, so the request-teardown
   * sweep has to know about them. Pins that {@code clear()} was extended alongside the new state
   * and cannot silently leave it bound to the thread.
   */
  @Test
  public void testClearRemovesAuditExtras() {
    RequestContext.setAuditExtras(Collections.singletonMap("k", "v"));
    RequestContext.clear();
    Assertions.assertTrue(RequestContext.takeAuditExtras().isEmpty());
  }

  /**
   * The stash is only safe because a contributor and the dispatcher share one servlet thread. Pins
   * that a fact set on one thread is invisible to another, which is the assumption that makes a
   * {@link ThreadLocal} acceptable here in place of a threaded-through parameter.
   */
  @Test
  public void testAuditExtrasAreThreadConfined() throws InterruptedException {
    RequestContext.setAuditExtras(Collections.singletonMap("audit.reason", "policy-applied"));
    AtomicReference<Map<String, String>> childValue = new AtomicReference<>();

    Thread child = new Thread(() -> childValue.set(RequestContext.takeAuditExtras()));
    child.start();
    child.join();

    Assertions.assertTrue(childValue.get().isEmpty(), "Child thread must not see stashed extras");
    Assertions.assertEquals(
        "policy-applied",
        RequestContext.takeAuditExtras().get("audit.reason"),
        "Child thread must not consume the parent's stash");
  }

  /**
   * Unlike audit extras, the query-param snapshot is read by every {@code Event} constructed during
   * the request, not consumed once. Pins that reading it does not clear it.
   */
  @Test
  public void testGetRequestQueryParamsDoesNotClear() {
    RequestContext.setRequestQueryParams(Collections.singletonMap("details", "true"));
    Assertions.assertEquals("true", RequestContext.getRequestQueryParams().get("details"));
    Assertions.assertEquals(
        "true",
        RequestContext.getRequestQueryParams().get("details"),
        "a second read must still see the value");
  }

  @Test
  public void testGetRequestQueryParamsReturnsEmptyWhenUnset() {
    Assertions.assertTrue(RequestContext.getRequestQueryParams().isEmpty());
  }

  @Test
  public void testEmptyOrNullRequestQueryParamsClearsStash() {
    RequestContext.setRequestQueryParams(Collections.singletonMap("k", "v"));
    RequestContext.setRequestQueryParams(Collections.emptyMap());
    Assertions.assertTrue(RequestContext.getRequestQueryParams().isEmpty());

    RequestContext.setRequestQueryParams(Collections.singletonMap("k", "v"));
    RequestContext.setRequestQueryParams(null);
    Assertions.assertTrue(RequestContext.getRequestQueryParams().isEmpty());
  }

  @Test
  public void testClearRemovesRequestQueryParams() {
    RequestContext.setRequestQueryParams(Collections.singletonMap("k", "v"));
    RequestContext.clear();
    Assertions.assertTrue(RequestContext.getRequestQueryParams().isEmpty());
  }

  /**
   * Mirrors the existing {@code operationFailureFired} flag on the success path. Kept as an
   * independent flag (not merged with the failure one) so that an operation which succeeds but
   * whose HTTP response delivery later fails can still produce both a success and a failure audit
   * entry.
   */
  @Test
  public void testOperationSuccessFiredLifecycle() {
    Assertions.assertFalse(RequestContext.isOperationSuccessFired());
    RequestContext.markOperationSuccessFired();
    Assertions.assertTrue(RequestContext.isOperationSuccessFired());
    RequestContext.resetOperationSuccessFired();
    Assertions.assertFalse(RequestContext.isOperationSuccessFired());
  }

  @Test
  public void testClearRemovesOperationSuccessFired() {
    RequestContext.markOperationSuccessFired();
    RequestContext.clear();
    Assertions.assertFalse(RequestContext.isOperationSuccessFired());
  }

  /**
   * Failure is sticky: a success dispatch that happens to follow a failure dispatch on the same
   * thread (not expected in practice, but not structurally prevented either) must not overwrite it
   * — otherwise HttpAuditFilter would see isOperationFailureFired() == false and double-log a
   * 4xx/5xx response with its own fallback failure event, on top of the real operation-layer one.
   */
  @Test
  public void testMarkOperationSuccessFiredDoesNotOverwriteFailure() {
    RequestContext.markOperationFailureFired();
    RequestContext.markOperationSuccessFired();
    Assertions.assertTrue(RequestContext.isOperationFailureFired(), "failure must remain recorded");
    Assertions.assertFalse(
        RequestContext.isOperationSuccessFired(), "success must not overwrite a recorded failure");
  }
>>>>>>> 15259af5d ([#12872] fix(core): Capture and redact request query parameters in audit log entries (#12891))
}
