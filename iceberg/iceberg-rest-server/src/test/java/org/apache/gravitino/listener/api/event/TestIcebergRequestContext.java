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

package org.apache.gravitino.listener.api.event;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.Enumeration;
import javax.servlet.http.HttpServletRequest;
import org.apache.gravitino.NameIdentifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestIcebergRequestContext {

  @Test
  void testNoHeaderIsSync() {
    Assertions.assertFalse(new IcebergRequestContext(requestWithoutHeader(), "cat").asyncPurge());
  }

  @Test
  void testTrueHeaderIsAsync() {
    Assertions.assertTrue(
        new IcebergRequestContext(requestWithAsyncPurgeHeader(" true "), "cat").asyncPurge());
  }

  @Test
  void testFalseHeaderIsSync() {
    Assertions.assertFalse(
        new IcebergRequestContext(requestWithAsyncPurgeHeader("false"), "cat").asyncPurge());
  }

  @Test
  void testGarbageHeaderIsSync() {
    Assertions.assertFalse(
        new IcebergRequestContext(requestWithAsyncPurgeHeader("yes"), "cat").asyncPurge());
  }

  @Test
  void testUppercaseValueIsSync() {
    // HTTP header values are case-sensitive; only the exact value "true" opts in.
    Assertions.assertFalse(
        new IcebergRequestContext(requestWithAsyncPurgeHeader("True"), "cat").asyncPurge());
  }

  @Test
  void testHeaderNameIsCaseInsensitive() {
    Assertions.assertTrue(
        new IcebergRequestContext(requestWithHeader("x-gravitino-async-purge", "true"), "cat")
            .asyncPurge());
  }

  /**
   * Extras are a parallel map, not headers. A later audit formatter that reads {@code
   * httpHeaders()} must not see a fact that was never on the wire.
   */
  @Test
  void testExtrasDoNotAppearOnHttpHeaders() {
    IcebergRequestContext original =
        new IcebergRequestContext(requestWithHeader("X-Request-Id", "req-1"), "cat");
    IcebergRequestContext enriched =
        original.withAuditExtras(ImmutableMap.of("audit.reason", "policy-applied"));

    Assertions.assertNotSame(original, enriched);
    Assertions.assertTrue(original.auditExtras().isEmpty());
    Assertions.assertEquals("policy-applied", enriched.auditExtras().get("audit.reason"));
    Assertions.assertFalse(enriched.httpHeaders().containsKey("audit.reason"));
    Assertions.assertEquals("req-1", enriched.httpHeaders().get("X-Request-Id"));
  }

  /**
   * Today's {@code customInfo()} is the headers map. Empty extras have to keep that identity so
   * existing callers do not observe a copy.
   */
  @Test
  void testCustomInfoIsHeadersWhenExtrasEmpty() {
    IcebergRequestContext context =
        new IcebergRequestContext(requestWithHeader("X-Request-Id", "req-1"), "cat");
    Assertions.assertSame(context.httpHeaders(), context.customInfo());
    Assertions.assertSame(
        context.httpHeaders(), context.withAuditExtras(ImmutableMap.of()).customInfo());
    Assertions.assertSame(context.httpHeaders(), context.withAuditExtras(null).customInfo());
  }

  @Test
  void testCustomInfoMergesHeadersAndExtras() {
    IcebergRequestContext context =
        new IcebergRequestContext(requestWithHeader("X-Request-Id", "req-1"), "cat")
            .withAuditExtras(ImmutableMap.of("audit.reason", "policy-applied"));
    Assertions.assertEquals("req-1", context.customInfo().get("X-Request-Id"));
    Assertions.assertEquals("policy-applied", context.customInfo().get("audit.reason"));
    Assertions.assertFalse(context.httpHeaders().containsKey("audit.reason"));
  }

  /**
   * Failure events are the case where the reason matters most. Pins that {@link
   * IcebergFailureEvent#customInfo()} is headers ∪ extras and that extras stay off {@code
   * httpHeaders()}.
   */
  @Test
  void testFailureEventCustomInfoMergesHeadersAndExtras() {
    IcebergRequestContext context =
        new IcebergRequestContext(requestWithHeader("X-Request-Id", "req-1"), "cat")
            .withAuditExtras(ImmutableMap.of("audit.reason", "validation-failed"));
    IcebergLoadTableFailureEvent event =
        new IcebergLoadTableFailureEvent(
            context, NameIdentifier.of("ml", "cat", "ns", "t"), new RuntimeException("boom"));

    Assertions.assertEquals("req-1", event.customInfo().get("X-Request-Id"));
    Assertions.assertEquals("validation-failed", event.customInfo().get("audit.reason"));
    Assertions.assertFalse(event.icebergRequestContext().httpHeaders().containsKey("audit.reason"));
  }

  @Test
  void testFailureEventCustomInfoIsHeaderOnlyWhenExtrasEmpty() {
    IcebergRequestContext context =
        new IcebergRequestContext(requestWithHeader("X-Request-Id", "req-1"), "cat");
    IcebergLoadTableFailureEvent event =
        new IcebergLoadTableFailureEvent(
            context, NameIdentifier.of("ml", "cat", "ns", "t"), new RuntimeException("boom"));
    Assertions.assertSame(context.httpHeaders(), event.customInfo());
  }

  private static HttpServletRequest requestWithoutHeader() {
    HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getRemoteHost()).thenReturn("localhost");
    when(request.getHeaderNames()).thenReturn(Collections.emptyEnumeration());
    return request;
  }

  private static HttpServletRequest requestWithAsyncPurgeHeader(String value) {
    return requestWithHeader(IcebergRequestContext.ASYNC_PURGE_HEADER, value);
  }

  private static HttpServletRequest requestWithHeader(String name, String value) {
    HttpServletRequest request = mock(HttpServletRequest.class);
    Enumeration<String> headerNames = Collections.enumeration(Collections.singleton(name));
    when(request.getRemoteHost()).thenReturn("localhost");
    when(request.getHeaderNames()).thenReturn(headerNames);
    when(request.getHeader(name)).thenReturn(value);
    return request;
  }
}
