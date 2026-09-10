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

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestExceptionMessages {

  @Test
  public void testUsefulMessageUnwrapsTransparentWrappers() {
    Throwable root = new IllegalArgumentException("root reason");
    Throwable mid = new ExecutionException(root);
    Throwable top = new RuntimeException(mid);

    Assertions.assertEquals("root reason", ExceptionMessages.usefulMessage(top));
  }

  @Test
  public void testUsefulMessageKeepsImmediateContextAndAppendsDeeperReason() {
    Throwable root = new IllegalArgumentException("root reason");
    Throwable mid = new ExecutionException(root);
    Throwable top = new RuntimeException("wrapper", mid);

    Assertions.assertEquals("wrapper: root reason", ExceptionMessages.usefulMessage(top));
  }

  @Test
  public void testUsefulMessageIgnoresBlankMessages() {
    Throwable blankCause = new IOException("   ");
    Throwable cause = new IOException("HMS connection refused", blankCause);

    Assertions.assertEquals(
        "Failed to load table: HMS connection refused",
        ExceptionMessages.withCause("Failed to load table", cause));
    Assertions.assertEquals("HMS connection refused", ExceptionMessages.usefulMessage(cause));
  }

  @Test
  public void testUsefulMessageBlankOnlyChainReturnsNull() {
    Throwable blank = new IOException("\n\t ");
    Assertions.assertNull(ExceptionMessages.usefulMessage(blank));
    Assertions.assertEquals(
        "Failed to load table", ExceptionMessages.withCause("Failed to load table", blank));
  }

  @Test
  public void testUsefulMessageStopsOnTwoNodeCycle() {
    RuntimeException a = new RuntimeException("a");
    RuntimeException b = new RuntimeException("b");
    a.initCause(b);
    b.initCause(a);

    Assertions.assertEquals("a: b", ExceptionMessages.usefulMessage(a));
  }

  @Test
  public void testUsefulMessageStopsOnThreeNodeCycle() {
    RuntimeException a = new RuntimeException("a");
    RuntimeException b = new RuntimeException("b");
    RuntimeException c = new RuntimeException("c");
    a.initCause(b);
    b.initCause(c);
    c.initCause(a);

    Assertions.assertEquals("a: c", ExceptionMessages.usefulMessage(a));
  }

  @Test
  public void testNestedWrapPreservesIntermediatePropertyContext() {
    Throwable root = new IOException("write failed");
    Throwable inner = ExceptionMessages.wrap("Failed to write property: fs.s3a.endpoint", root);
    RuntimeException outer = ExceptionMessages.wrap("Failed to create configuration", inner);

    Assertions.assertTrue(
        outer.getMessage().contains("fs.s3a.endpoint"),
        "nested wrap must keep intermediate property context, got: " + outer.getMessage());
    Assertions.assertEquals(
        "Failed to create configuration: Failed to write property: fs.s3a.endpoint: write failed",
        outer.getMessage());
  }

  @Test
  public void testWithCauseAppendsUpstreamMessage() {
    Throwable cause =
        new IllegalArgumentException(
            "Invalid value nonsense for configuration cleanup.policy: String must be one of:"
                + " compact, delete");

    String combined =
        ExceptionMessages.withCause("Failed to alter topic properties for topic prop_probe", cause);

    Assertions.assertEquals(
        "Failed to alter topic properties for topic prop_probe: Invalid value nonsense for"
            + " configuration cleanup.policy: String must be one of: compact, delete",
        combined);
  }

  @Test
  public void testWithCauseDoesNotDuplicateMessage() {
    String context = "Failed to alter topic: bad value";
    Throwable cause = new IllegalArgumentException("bad value");

    Assertions.assertEquals(context, ExceptionMessages.withCause(context, cause));
  }

  @Test
  public void testWrapPreservesCauseAndMessage() {
    Throwable cause = new IllegalStateException("glue denied");
    RuntimeException wrapped = ExceptionMessages.wrap("Glue error: schema drop_me", cause);

    Assertions.assertEquals("Glue error: schema drop_me: glue denied", wrapped.getMessage());
    Assertions.assertSame(cause, wrapped.getCause());
  }

  @Test
  public void testIllegalArgumentPreservesCauseAndMessage() {
    Throwable cause = new IllegalArgumentException("not allowed");
    IllegalArgumentException wrapped =
        ExceptionMessages.illegalArgument("Invalid properties for topic t1", cause);

    Assertions.assertEquals("Invalid properties for topic t1: not allowed", wrapped.getMessage());
    Assertions.assertSame(cause, wrapped.getCause());
  }
}
