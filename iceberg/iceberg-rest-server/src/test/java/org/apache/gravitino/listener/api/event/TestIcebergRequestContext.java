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

import java.util.Collections;
import java.util.Enumeration;
import javax.servlet.http.HttpServletRequest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestIcebergRequestContext {

  @Test
  void testAsyncPurgeAbsentByDefault() {
    IcebergRequestContext context = new IcebergRequestContext(requestWithoutHeader(), "cat");

    Assertions.assertFalse(context.asyncPurge());
  }

  @Test
  void testAsyncPurgeTrueHeaderOptsIn() {
    IcebergRequestContext context =
        new IcebergRequestContext(requestWithAsyncPurgeHeader(" true "), "cat");

    Assertions.assertTrue(context.asyncPurge());
  }

  @Test
  void testAsyncPurgeFalseHeaderStaysSync() {
    IcebergRequestContext context =
        new IcebergRequestContext(requestWithAsyncPurgeHeader("false"), "cat");

    Assertions.assertFalse(context.asyncPurge());
  }

  @Test
  void testAsyncPurgeGarbageHeaderStaysSync() {
    IcebergRequestContext context =
        new IcebergRequestContext(requestWithAsyncPurgeHeader("yes"), "cat");

    Assertions.assertFalse(context.asyncPurge());
  }

  @Test
  void testAsyncPurgeHeaderLookupIgnoresCase() {
    IcebergRequestContext context =
        new IcebergRequestContext(requestWithHeader("x-gravitino-async-purge", "true"), "cat");

    Assertions.assertTrue(context.asyncPurge());
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
