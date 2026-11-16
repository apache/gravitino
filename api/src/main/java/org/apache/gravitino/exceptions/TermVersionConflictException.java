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
package org.apache.gravitino.exceptions;

import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.FormatString;

/**
 * A conditional edit or binding cursor refers to a different term identity or revision.
 *
 * <p>A same-name replacement can conflict even at the same numeric revision. Reload and reconcile
 * before explicitly resubmitting; do not blindly replay the stale operation. REST maps this
 * optimistic-lock conflict to HTTP 409.
 */
public class TermVersionConflictException extends OptimisticLockException {

  /**
   * Constructs a conflict with a formatted detail message.
   *
   * @param message The format string.
   * @param args The format arguments.
   */
  @FormatMethod
  public TermVersionConflictException(@FormatString String message, Object... args) {
    super(message, args);
  }

  /**
   * Constructs a conflict with its cause and a formatted detail message.
   *
   * @param cause The underlying cause.
   * @param message The format string.
   * @param args The format arguments.
   */
  @FormatMethod
  public TermVersionConflictException(
      Throwable cause, @FormatString String message, Object... args) {
    super(cause, message, args);
  }
}
