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

import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestExceptionMessages {

  @Test
  public void testUsefulMessagePrefersDeepestCause() {
    Throwable root = new IllegalArgumentException("root reason");
    Throwable mid = new ExecutionException(root);
    Throwable top = new RuntimeException("wrapper", mid);

    Assertions.assertEquals("root reason", ExceptionMessages.usefulMessage(top));
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
