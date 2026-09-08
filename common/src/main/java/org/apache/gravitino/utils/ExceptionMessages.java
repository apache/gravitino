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

import javax.annotation.Nullable;

/**
 * Helpers for preserving underlying system error messages when wrapping exceptions.
 *
 * <p>Catalog and server code often adds operation context when rethrowing. Context is useful, but
 * it must not replace the upstream message that operators need to act on.
 */
public final class ExceptionMessages {

  private ExceptionMessages() {}

  /**
   * Returns the most specific non-blank message from {@code throwable} or its cause chain.
   *
   * @param throwable the throwable to inspect, may be null
   * @return the deepest useful message, or null if none is available
   */
  @Nullable
  public static String usefulMessage(@Nullable Throwable throwable) {
    if (throwable == null) {
      return null;
    }

    String lastUseful = null;
    Throwable current = throwable;
    while (current != null) {
      String message = current.getMessage();
      if (message != null && !message.isEmpty()) {
        lastUseful = message;
      }
      Throwable cause = current.getCause();
      if (cause == null || cause == current) {
        break;
      }
      current = cause;
    }
    return lastUseful;
  }

  /**
   * Combines operation context with the underlying cause message.
   *
   * <p>If {@code context} already contains the useful cause message, {@code context} is returned
   * unchanged. If there is no useful cause message, {@code context} is returned as-is.
   *
   * @param context operation/object context such as {@code "Failed to alter topic X"}
   * @param throwable the underlying failure
   * @return a message that preserves both context and the upstream reason when available
   */
  public static String withCause(String context, @Nullable Throwable throwable) {
    String useful = usefulMessage(throwable);
    if (useful == null || useful.isEmpty()) {
      return context;
    }
    if (context == null || context.isEmpty()) {
      return useful;
    }
    if (context.contains(useful)) {
      return context;
    }
    return context + ": " + useful;
  }

  /**
   * Wraps {@code throwable} in a {@link RuntimeException} whose message includes both {@code
   * context} and the underlying cause message.
   *
   * @param context operation/object context
   * @param throwable the underlying failure
   * @return a runtime exception suitable for rethrowing from catalog operations
   */
  public static RuntimeException wrap(String context, Throwable throwable) {
    return new RuntimeException(withCause(context, throwable), throwable);
  }

  /**
   * Creates an {@link IllegalArgumentException} whose message includes both {@code context} and the
   * underlying cause message.
   *
   * @param context operation/object context
   * @param throwable the underlying failure
   * @return an illegal-argument exception for client-caused failures
   */
  public static IllegalArgumentException illegalArgument(String context, Throwable throwable) {
    return new IllegalArgumentException(withCause(context, throwable), throwable);
  }
}
